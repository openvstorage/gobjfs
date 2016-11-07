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


#include <gtest/gtest.h>
#include <rora/EdgeQueue.h>
#include <rora/GatewayProtocol.h>

using namespace gobjfs::rora;

// Can EdgeQueue handle multiple Pralayas?
TEST(EdgeQueueTest, CreatorDestroyer) {
  
  int pid = 10;
  size_t maxQueueLen = 100;
  size_t maxMsgSize = 100;
  size_t maxAllocSize = 4096;

  EdgeQueue::remove(pid);

  for (int idx = 0; idx < 10; idx ++) {
    // 2nd creation should succeed if resources properly destroyed
    EdgeQueue *creator = new EdgeQueue(pid, maxQueueLen, maxMsgSize, maxAllocSize);
    delete creator;
  }
}

TEST(EdgeQueueTest, DuplicateCreation) {
  
  int pid = 10;
  size_t maxQueueLen = 100;
  size_t maxMsgSize = 100;
  size_t maxAllocSize = 4096;

  EdgeQueue::remove(pid);

  EdgeQueue *creator1 = new EdgeQueue(pid, maxQueueLen, maxMsgSize, maxAllocSize);
  // try to create message queue and shmem segment with same pid again
  // it must fail
  EXPECT_THROW({
    EdgeQueue *creator2 = new EdgeQueue(pid, maxQueueLen, maxMsgSize, maxAllocSize);
    delete creator2;
  }, std::runtime_error);
  
  delete creator1;
}

TEST(EdgeQueueTest, NonExistingQueue) {

  int pid = 10;
  EdgeQueue::remove(pid);
  EXPECT_THROW({
    EdgeQueue *reader = new EdgeQueue(pid);
    delete reader;
  }, std::runtime_error);
}


TEST(EdgeQueueTest, ReaderWriterQueue) {
  
  int pid = 10;
  size_t maxQueueLen = 100;
  size_t maxMsgSize = 100;
  size_t maxAllocSize = 4096;

  EdgeQueue::remove(pid);
  EdgeQueue *creator = new EdgeQueue(pid, maxQueueLen, maxMsgSize, maxAllocSize);
  EdgeQueue *reader = new EdgeQueue(pid);

  for (size_t idx = 0; idx < maxQueueLen; idx ++)
  {
    GatewayMsg msg;
    msg.offsetVec_.push_back(idx);
    msg.filenameVec_.push_back("abcd");
    auto ret = creator->writeResponse(msg);
    EXPECT_EQ(ret, 0);
  }

  // both shared memory queue handles should report same queue len
  EXPECT_EQ(creator->getResponseCurrentQueueLen(), maxMsgSize);
  EXPECT_EQ(reader->getResponseCurrentQueueLen(), maxMsgSize);

  for (size_t idx = 0; idx < maxQueueLen; idx ++)
  {
    GatewayMsg msg;
    auto ret = reader->readResponse(msg);
    EXPECT_EQ(ret, 0);
    ASSERT_EQ(msg.offsetVec_[0], idx);
    ASSERT_EQ(msg.filenameVec_[0], "abcd");

    EXPECT_EQ(creator->getResponseCurrentQueueLen(), maxMsgSize - (idx + 1));
    EXPECT_EQ(reader->getResponseCurrentQueueLen(), maxMsgSize - (idx + 1));
  }

  delete creator;
  delete reader;
}

TEST(EdgeQueueTest, SegmentAllocFree) {
  
  int pid = 10;
  size_t maxQueueLen = 100;
  size_t maxMsgSize = 100;
  size_t maxAllocSize = 4096;

  EdgeQueue::remove(pid);
  EdgeQueue *creator = new EdgeQueue(pid, maxQueueLen, maxMsgSize, maxAllocSize);

  // shared mem allocator uses some space for header segments
  // so cant get exact match
  size_t totalMem = creator->getFreeMem();
  EXPECT_GE(totalMem, (maxQueueLen - 1) * maxAllocSize);

  std::vector<void*> allocPtrs;
  for (size_t idx = 0; idx < maxQueueLen; idx ++) {
    void* buffer = creator->alloc(maxAllocSize);
    allocPtrs.push_back(buffer);
    if (creator->getFreeMem() < maxAllocSize) {
      // shared mem allocator uses some space for header segments
      // so cannot allocate as much as configured
      break;
    }
  }

  for (auto ptr : allocPtrs) {
    int ret = creator->free(ptr);
    EXPECT_EQ(ret, 0);
  }
  allocPtrs.clear();

  delete creator;
}

TEST(EdgeQueueTest, NoAllocFreeByReader) {

  int pid = 10;
  size_t maxQueueLen = 100;
  size_t maxMsgSize = 100;
  size_t maxAllocSize = 4096;

  EdgeQueue::remove(pid);

  EdgeQueue *creator = new EdgeQueue(pid, maxQueueLen, maxMsgSize, maxAllocSize);

  EdgeQueue *reader = new EdgeQueue(pid);
  {
  // check reader not allowed to alloc shmem
    void* buffer = reader->alloc(maxAllocSize);
    EXPECT_EQ(buffer, nullptr);
  }
  {
    void* buffer = creator->alloc(maxAllocSize);
  // check reader not allowed to free shmem
    int ret = reader->free(buffer);
    EXPECT_NE(ret, 0);
    ret = creator->free(buffer);
    EXPECT_EQ(ret, 0);
  }

  delete reader;
  delete creator;
}
