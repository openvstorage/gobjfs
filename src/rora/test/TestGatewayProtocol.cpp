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
#include <rora/GatewayProtocol.h>
#include <rora/EdgeQueue.h>

using namespace gobjfs::rora;

// can gateway msg encode shared ptr
TEST(GatewayProtocolTest, SharedPtrInMsgPack) {

  int pid = 10;
  size_t maxQueueLen = 100;
  size_t maxMsgSize = 1024;
  size_t maxAllocSize = 4096;

  EdgeQueue *creator = new EdgeQueue(pid, 
      maxQueueLen, 
      maxMsgSize, 
      maxAllocSize);

  // create an EdgeQueue
  void* sendBufPtr = creator->alloc(maxAllocSize);
  memset(sendBufPtr, 'a', maxAllocSize);

  GatewayMsg sendMsg;
  // get the handle from the ptr
  sendMsg.bufVec_.push_back(creator->segment_->get_handle_from_address(sendBufPtr));
  // write into message queue
  auto ret = creator->writeResponse(sendMsg);
  EXPECT_EQ(ret, 0);

  EdgeQueue *reader = new EdgeQueue(pid);

  // read from message queue
  GatewayMsg recvMsg;
  ret = reader->readResponse(recvMsg);
  EXPECT_EQ(ret, 0);

  // recover the ptr from the handle
  void* recvBufPtr = reader->segment_->get_address_from_handle(recvMsg.bufVec_[0]);

  // check if it has the same memory contents
  int mem_ret = memcmp(recvBufPtr, sendBufPtr, maxAllocSize);
  EXPECT_EQ(mem_ret, 0);

  creator->free(sendBufPtr);

  delete reader;
  delete creator;
}

