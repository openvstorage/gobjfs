/*
Copyright (C) 2016 iNuron NV

This file is part of Open vStorage Open Source Edition (OSE), as available from


    http://www.openvstorage.org and
    http://www.openvstorage.com.

This file is free software; you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
as published by the Free Software Foundation, in version 3 as it comes
in the <LICENSE.txt> file of the Open vStorage OSE distribution.

Open vStorage is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY of any kind.
*/

#include <util/os_utils.h>
#include <gIOExecFile.h>
#include <gMempool.h>
#include <gobjfs_client.h>
#include <gobjfs_server.h>
#include <networkxio/NetworkXioServer.h>

#include <gobjfs_log.h>
#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/types.h>
#include <future>
#include <thread>

using namespace gobjfs::xio;

static int fileTranslatorFunc(const char *old_name, size_t old_length,
                              char *new_name);

class MultiServerTest : public testing::Test {

  int fd{-1};

public:
  static constexpr size_t NUM_SERVERS = 2;
  static constexpr size_t NUM_CLIENTS = 5;
  static constexpr int FIRST_PORT = 21321;

  char configFile[512];

  gobjfs_xio_server_handle xs[NUM_SERVERS];
  std::future<void> fut[NUM_SERVERS];
  int portNumber[NUM_SERVERS];

  int testDataFd{gobjfs::os::FD_INVALID};
  static const std::string testDataFilePath;
  static const std::string testDataFileName;
  static const std::string testDataFileFullName;

  MultiServerTest() {
    for (size_t idx = 0; idx < NUM_SERVERS; idx++) {
      // need to space ports apart because portals take up consecutive ports
      portNumber[idx] = FIRST_PORT + (100 * idx);
    }
  }

  virtual void SetUp() override {

    strcpy(configFile, "ioexecfiletestXXXXXX");

    fd = mkstemp(configFile);

    const char *configContents = "[ioexec]\n"
                                 "ctx_queue_depth=200\n"
                                 "cpu_core=0\n";

    ssize_t writeSz = write(fd, configContents, strlen(configContents));

    EXPECT_EQ(writeSz, strlen(configContents));

    bool newInstance = true;
    for (size_t idx = 0; idx < NUM_SERVERS; idx++) {
      xs[idx] = gobjfs_xio_server_start("tcp", "127.0.0.1", portNumber[idx], 1, 1,
                                        200, fileTranslatorFunc, newInstance);
      EXPECT_NE(xs[idx], nullptr);
    }
  }

  virtual void TearDown() override {

    int ret = 0;
    for (size_t idx = 0; idx < NUM_SERVERS; idx++) {
      ret = gobjfs_xio_server_stop(xs[idx]);
      EXPECT_EQ(ret, 0);
    }

    {
      ret = close(fd);
      ASSERT_EQ(ret, 0);
      ret = ::unlink(configFile);
      ASSERT_EQ(ret, 0);
    }
  }

  void createDataFile() {

    int fd = creat(testDataFileFullName.c_str(), S_IRUSR | S_IWUSR);
    EXPECT_GE(fd, 0);

    const size_t bufSize = 65536 * 1024;
    char* buf = (char*)malloc(bufSize);
    memset(buf, 'a', bufSize);

    ssize_t ret = write(fd, buf, bufSize);
    EXPECT_EQ(ret, bufSize);

    ret = close(fd);
    EXPECT_EQ(ret, 0);
  }

  void removeDataFile(bool check = true) {

    int ret = ::unlink(testDataFileFullName.c_str());
    if (check)
      ASSERT_EQ(ret, 0);
  }

  virtual ~MultiServerTest() {}
};

const std::string MultiServerTest::testDataFilePath = "/tmp/";
const std::string MultiServerTest::testDataFileName = "abcd";
const std::string MultiServerTest::testDataFileFullName =
    std::string(MultiServerTest::testDataFilePath) +
    std::string(MultiServerTest::testDataFileName);

int fileTranslatorFunc(const char *old_name, size_t old_length,
                       char *new_name) {
  strcpy(new_name, MultiServerTest::testDataFilePath.c_str());
  strncat(new_name, old_name, old_length);
  return 0;
}

TEST_F(MultiServerTest, MultiServers) {

  client_ctx_attr_ptr ctx_attr[NUM_SERVERS];

  for (size_t idx = 0; idx < NUM_SERVERS; idx++) {

    ctx_attr[idx] = ctx_attr_new();

    ctx_attr_set_transport(ctx_attr[idx], "tcp", "127.0.0.1", portNumber[idx]);
  }

  std::vector<client_ctx_ptr> ptr_vec;

  for (size_t idx = 0; idx < NUM_SERVERS; idx++) {

    for (size_t i = 0; i < NUM_CLIENTS; i++) {
      auto ctx = ctx_new(ctx_attr[idx]);
      EXPECT_NE(ctx, nullptr);

      int err = ctx_init(ctx);
      EXPECT_EQ(err, 0);

      ptr_vec.push_back(ctx);
    }
  }

  for (auto &ctx_ptr : ptr_vec) {
    // disconnect the client from server
    ctx_ptr.reset();
  }
}
