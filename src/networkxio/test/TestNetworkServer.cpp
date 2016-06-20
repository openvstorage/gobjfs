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

  char configFile[512];

  NetworkXioServer* xs;
  std::future<void> fut;
  int portNumber{21321};

  int testDataFd {gobjfs::os::FD_INVALID};
  std::string testDataFile = "/tmp/ioexectest/dir0/abcd";

  NetworkXioServerTest() {
  }

  virtual void SetUp() override {

    strcpy(configFile,  "ioexecfiletestXXXXXX");

    fd = mkstemp(configFile);

    const char* configContents = 
      "[ioexec]\n"
      "ctx_queue_depth=200\n"
      "cpu_core=0\n"
      "[file_distributor]\n"
      "mount_point=/tmp/ioexectest\n"
      "num_dirs=1\n"
      ;

    ssize_t writeSz = write(fd, configContents, strlen(configContents));

    EXPECT_EQ(writeSz, strlen(configContents));

    std::string url = "tcp://127.0.0.1:" + std::to_string(portNumber);
    bool newInstance = true;
  
    xs = new NetworkXioServer(url, configFile, newInstance);
  
    std::promise<void> pr;
    auto lock_fut = pr.get_future();
  
    fut = std::async(std::launch::async,
        [&] () { xs->run(pr); });
  
    lock_fut.wait();

  }

  virtual void TearDown() override {

    int ret = 0;
    xs->shutdown();

    fut.wait();

    {
      ret = close(fd);
      ASSERT_EQ(ret, 0);
      ret = ::unlink(configFile);
      ASSERT_EQ(ret, 0);
    }

  }

  void createDataFile() {

    testDataFd = open(testDataFile.c_str(), O_TRUNC | O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

    const size_t bufSize = 65536 * 1024;
    char* buf = (char*)malloc(bufSize);
    bzero(buf, bufSize);

    ssize_t ret = write(testDataFd, buf, bufSize);
    EXPECT_EQ(ret, bufSize);

    free(buf);

    ret = close(testDataFd);
    ASSERT_EQ(ret, 0);
  }

  void removeDataFile() {

    int ret = ::unlink(testDataFile.c_str());
    ASSERT_EQ(ret, 0);

  }


  virtual ~NetworkXioServerTest() {
  }
};

TEST_F(NetworkXioServerTest, MultipleClients) {

  auto ctx_attr = ctx_attr_new();

  ctx_attr_set_transport(ctx_attr,
                                       "tcp",
                                       "127.0.0.1",
                                       portNumber);

  std::vector<client_ctx_ptr> ptr_vec;

  for (int i = 0; i < 100; i++) {
    auto ctx = ctx_new(ctx_attr);
    EXPECT_NE(ctx, nullptr);

    int err = ctx_init(ctx);
    EXPECT_EQ(err, 0);

    ptr_vec.push_back(ctx);
  }

  for (auto& ctx_ptr : ptr_vec) {
    // disconnect the client from server
    ctx_ptr.reset();
  }
}

TEST_F(NetworkXioServerTest, FileDoesntExist) {

  static constexpr size_t BufferSize = 512;
  // shorten read size to test unaligned reads
  static constexpr size_t ShortenSize = 10;

  size_t times = 1; // TODO debug why more than 1 fails

  auto ctx_attr = ctx_attr_new();

  ctx_attr_set_transport(ctx_attr,
                                       "tcp",
                                       "127.0.0.1",
                                       portNumber);

  client_ctx_ptr ctx = ctx_new(ctx_attr);
  EXPECT_NE(ctx, nullptr);

  int err = ctx_init(ctx);
  EXPECT_EQ(err, 0);

  for (decltype(times) i = 0; i < times; i ++) {

    auto rbuf = (char*)malloc((i+1) * BufferSize);
    EXPECT_NE(rbuf, nullptr);

    size_t readSz = BufferSize - ShortenSize;

    auto sz = gobjfs::xio::read(ctx, "abcd", rbuf, readSz, i * BufferSize);

    // reads will fail since file doesnt exist
    EXPECT_LT(sz, 0);

    free(rbuf);
  }

  ctx.reset();
}

TEST_F(NetworkXioServerTest, SyncRead) {

  createDataFile();

  static constexpr size_t BufferSize = 512;
  // shorten read size to test unaligned reads
  static constexpr size_t ShortenSize = 10;

  size_t times = 100; 

  auto ctx_attr = ctx_attr_new();

  ctx_attr_set_transport(ctx_attr,
                                       "tcp",
                                       "127.0.0.1",
                                       portNumber);

  client_ctx_ptr ctx = ctx_new(ctx_attr);
  EXPECT_NE(ctx, nullptr);

  int err = ctx_init(ctx);
  EXPECT_EQ(err, 0);

  for (decltype(times) i = 0; i < times; i ++) {

    auto rbuf = (char*)malloc(BufferSize);
    EXPECT_NE(rbuf, nullptr);

    size_t readSz = BufferSize - ShortenSize;

    auto sz = gobjfs::xio::read(ctx, "abcd", rbuf, readSz, i * BufferSize);

    // reads will fail since file doesnt exist
    EXPECT_EQ(sz, readSz);

    free(rbuf);
  }

  removeDataFile();

  ctx.reset();
}

TEST_F(NetworkXioServerTest, AsyncRead) {

  createDataFile();

  static constexpr size_t BufferSize = 512;
  // shorten read size to test unaligned reads
  static constexpr size_t ShortenSize = 10;

  size_t times = 100; 

  auto ctx_attr = ctx_attr_new();

  ctx_attr_set_transport(ctx_attr,
                                       "tcp",
                                       "127.0.0.1",
                                       portNumber);

  client_ctx_ptr ctx = ctx_new(ctx_attr);
  EXPECT_NE(ctx, nullptr);

  int err = ctx_init(ctx);
  EXPECT_EQ(err, 0);

  std::vector<giocb*> vec;

  size_t readSz = BufferSize - ShortenSize;

  for (decltype(times) i = 0; i < times; i ++) {


    auto rbuf = (char*)malloc(BufferSize);
    EXPECT_NE(rbuf, nullptr);

    giocb* iocb = (giocb*)malloc(sizeof(giocb));
    iocb->aio_buf = rbuf;
    iocb->aio_offset = i * BufferSize;
    iocb->aio_nbytes = readSz;

    auto ret = aio_readcb(ctx, "abcd", iocb, nullptr);
    EXPECT_EQ(ret, 0);

    vec.push_back(iocb);
  }

  for (auto& elem : vec) {

    auto ret = aio_suspend(ctx, elem, nullptr); 
    EXPECT_EQ(ret, 0);

    auto retcode = aio_return(ctx, elem);
    EXPECT_EQ(retcode, readSz);

    aio_finish(ctx, elem);
    free(elem->aio_buf);
    free(elem);
  }

  removeDataFile();

  ctx.reset();
}

TEST_F(NetworkXioServerTest, MultiAsyncRead) {

  createDataFile();

  static constexpr size_t BufferSize = 512;
  // shorten read size to test unaligned reads
  static constexpr size_t ShortenSize = 10;

  size_t times = 100; 

  auto ctx_attr = ctx_attr_new();

  ctx_attr_set_transport(ctx_attr,
                                       "tcp",
                                       "127.0.0.1",
                                       portNumber);

  client_ctx_ptr ctx = ctx_new(ctx_attr);
  EXPECT_NE(ctx, nullptr);

  int err = ctx_init(ctx);
  EXPECT_EQ(err, 0);

  std::vector<giocb*> iocb_vec;
  std::vector<std::string> filename_vec;

  size_t readSz = BufferSize - ShortenSize;

  for (decltype(times) i = 0; i < times; i ++) {

    auto rbuf = (char*)malloc(BufferSize);
    EXPECT_NE(rbuf, nullptr);


    giocb* iocb = (giocb*)malloc(sizeof(giocb));
    iocb->aio_buf = rbuf;
    iocb->aio_offset = i * BufferSize;
    iocb->aio_nbytes = readSz;

    iocb_vec.push_back(iocb);
    filename_vec.push_back("abcd");
  }

  auto ret = aio_readv(ctx, filename_vec, iocb_vec);
  EXPECT_EQ(ret, 0);

  ret = aio_suspendv(ctx, iocb_vec, nullptr); 
  EXPECT_EQ(ret, 0);

  for (auto& elem : iocb_vec) {
    auto retcode = aio_return(ctx, elem);
    EXPECT_EQ(retcode, readSz);
    aio_finish(ctx, elem);
    free(elem->aio_buf);
    free(elem);
  }

  removeDataFile();

  ctx.reset();
}
