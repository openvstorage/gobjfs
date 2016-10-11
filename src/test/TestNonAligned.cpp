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

#include <gobjfs_log.h>
#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/types.h>
#include <future>
#include <thread>

using namespace gobjfs::xio;

static int fileTranslator(const char *old_name, size_t old_length,
                          char *new_name);

class IOExecFileTest : public testing::Test {

  int configFileFd{-1};

public:
  char configFile[512];

  int testDataFd{gobjfs::os::FD_INVALID};
  static const std::string testDataFilePath;
  static const std::string testDataFileName;
  static const std::string testDataFileFullName;

  IOExecServiceHandle serviceHandle;
  IOExecEventFdHandle evHandle;
  int readFd;

  IOExecFileTest() {}

  void createConfigFile() {

    strcpy(configFile, "ioexecfiletestXXXXXX");

    configFileFd = mkstemp(configFile);

    const char *configContents = "[ioexec]\n"
                                 "ctx_queue_depth=200\n"
                                 "cpu_core=0\n";

    ssize_t writeSz =
        write(configFileFd, configContents, strlen(configContents));

    EXPECT_EQ(writeSz, strlen(configContents));
  }

  void deleteConfigFile() {
    int ret = close(configFileFd);
    ASSERT_EQ(ret, 0);
    ret = ::unlink(configFile);
    ASSERT_EQ(ret, 0);
  }

  void initService() {

    gMempool_init(512);

    serviceHandle = IOExecFileServiceInit(configFile, fileTranslator, true);

    ssize_t ret;

    evHandle = IOExecEventFdOpen(serviceHandle);
    EXPECT_NE(evHandle, nullptr);

    readFd = IOExecEventFdGetReadFd(evHandle);
    EXPECT_NE(readFd, gobjfs::os::FD_INVALID);
  }

  void destroyService() {

    IOExecEventFdClose(evHandle);

    IOExecFileServiceDestroy(serviceHandle);
  }

  virtual void SetUp() override {

    createConfigFile();

    initService();
  }

  virtual void TearDown() override {

    destroyService();

    deleteConfigFile();
  }

  virtual ~IOExecFileTest() {}
};

const std::string IOExecFileTest::testDataFilePath = "/tmp/";

const std::string IOExecFileTest::testDataFileName = "abcd";

const std::string IOExecFileTest::testDataFileFullName =
    std::string(IOExecFileTest::testDataFilePath) +
    std::string(IOExecFileTest::testDataFileName);

int fileTranslator(const char *old_name, size_t old_length, char *new_name) {
  strcpy(new_name, IOExecFileTest::testDataFilePath.c_str());
  strncat(new_name, old_name, old_length);
  return 0;
}

// Nonaligned write succeeds with files opened with O_DIRECT
TEST_F(IOExecFileTest, NonAlignedWriteWithoutDirectIO) {

  ssize_t ret = 0;
  char fillChar = 'a' + (getpid() % 26);

  // write the file
  {
    auto fileHandle =
        IOExecFileOpen(serviceHandle, testDataFileName.c_str(),
                       testDataFileName.size(), O_CREAT | O_WRONLY);

    auto batch = gIOBatchAlloc(1);
    gIOExecFragment &frag = batch->array[0];
    frag.offset = 0;
    const size_t bufSize = 65536 - 10;
    frag.size = bufSize;
    frag.addr = (char *)gMempool_alloc(bufSize);
    memset(frag.addr, fillChar, bufSize);
    frag.completionId = reinterpret_cast<uint64_t>(batch);

    ret = IOExecFileWrite(fileHandle, batch, evHandle);
    EXPECT_EQ(ret, 0);

    gIOStatus ioStatus;
    ret = ::read(readFd, &ioStatus, sizeof(ioStatus));
    EXPECT_EQ(ret, sizeof(ioStatus));
    EXPECT_EQ(ioStatus.errorCode, 0);
    EXPECT_EQ(ioStatus.completionId, reinterpret_cast<uint64_t>(batch));

    gIOBatchFree(batch);

    IOExecFileClose(fileHandle);
  }

  // read and verify buffer
  {
    auto fileHandle =
        IOExecFileOpen(serviceHandle, testDataFileName.c_str(),
                       testDataFileName.size(), O_DIRECT | O_CREAT | O_RDONLY);

    auto batch = gIOBatchAlloc(1);
    gIOExecFragment &frag = batch->array[0];
    frag.offset = 0;
    const size_t bufSize = 65536 - 10;
    frag.size = bufSize;
    frag.addr = (char *)gMempool_alloc(bufSize);
    frag.completionId = reinterpret_cast<uint64_t>(batch);

    ret = IOExecFileRead(fileHandle, batch, evHandle);
    EXPECT_EQ(ret, 0);

    gIOStatus ioStatus;
    ret = ::read(readFd, &ioStatus, sizeof(ioStatus));
    EXPECT_EQ(ret, sizeof(ioStatus));
    EXPECT_EQ(ioStatus.errorCode, 0);
    EXPECT_EQ(ioStatus.completionId, reinterpret_cast<uint64_t>(batch));

    for (size_t idx = 0; idx < bufSize; idx++) {
      EXPECT_EQ(frag.addr[idx], fillChar);
    }

    gIOBatchFree(batch);

    IOExecFileClose(fileHandle);
  }

  ret = ::unlink(testDataFileFullName.c_str());
  ASSERT_EQ(ret, 0);
}

// Nonaligned write fails with files opened with O_DIRECT
TEST_F(IOExecFileTest, NonAlignedWriteWithDirectIO) {

  auto fileHandle =
      IOExecFileOpen(serviceHandle, testDataFileName.c_str(),
                     testDataFileName.size(), O_DIRECT | O_CREAT | O_WRONLY);

  auto batch = gIOBatchAlloc(1);
  gIOExecFragment &frag = batch->array[0];
  frag.offset = 0;
  const size_t bufSize = 65536 - 10;
  frag.size = bufSize;
  frag.addr = (char *)gMempool_alloc(bufSize);
  memset(frag.addr, 'a', bufSize);
  frag.completionId = reinterpret_cast<uint64_t>(batch);

  int ret = IOExecFileWrite(fileHandle, batch, evHandle);
  EXPECT_EQ(ret, 0);

  gIOStatus ioStatus;
  ret = ::read(readFd, &ioStatus, sizeof(ioStatus));
  EXPECT_EQ(ret, sizeof(ioStatus));
  EXPECT_EQ(ioStatus.errorCode, -EINVAL);
  EXPECT_EQ(ioStatus.completionId, reinterpret_cast<uint64_t>(batch));

  gIOBatchFree(batch);

  IOExecFileClose(fileHandle);

  ret = ::unlink(testDataFileFullName.c_str());
  ASSERT_EQ(ret, 0);
}
