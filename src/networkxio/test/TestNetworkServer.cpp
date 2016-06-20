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

#include <gobjfs_client.h>
#include <networkxio/NetworkXioServer.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/types.h>
#include <future>
#include <thread>

using namespace gobjfs::xio;

class NetworkXioServerTest : public testing::Test {
  
  int fd {-1};

public:
  char fileTemplate[512];

  NetworkXioServerTest() {
  }

  virtual void SetUp() override {
    strcpy(fileTemplate,  "ioexecfiletestXXXXXX");

    fd = mkstemp(fileTemplate);

    const char* configContents = 
      "[ioexec]\n"
      "ctx_queue_depth=200\n"
      "cpu_core=0\n"
      "[file_distributor]\n"
      "mount_point=/tmp/ioexectest\n"
      "num_dirs=3\n"
      ;

    ssize_t writeSz = write(fd, configContents, strlen(configContents));

    EXPECT_EQ(writeSz, strlen(configContents));
  }

  virtual void TearDown() override {
    close(fd);
    int ret = ::unlink(fileTemplate);
    assert(ret == 0);
  }

  virtual ~NetworkXioServerTest() {
  }
};

// check if start-stop works
TEST_F(NetworkXioServerTest, StartStop) {

  std::string url = "tcp://127.0.0.1:21321";
  bool newInstance = true;

  auto xs = new NetworkXioServer(url, fileTemplate, newInstance);

  std::promise<void> pr;
  auto lock_fut = pr.get_future();

  auto fut = std::async(std::launch::async,
      [&] () { xs->run(pr); });

  lock_fut.wait();

  xs->shutdown();

  fut.wait();
}
