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

#include <gIOExecFile.h>
#include <gMempool.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/stat.h>
#include <sys/types.h>

#define TESTFILE "/tmp/objfs.tmp"

/*
 * TEST: verifyIO
 *	- Write Data to file
 *	- Make sure data is written, by re-reading it // TODO
 */
#define BLOCKSIZE 16384
#define ALIGNSIZE 4096
#define NUMIO 10

static int wait_for_iocompletion(int epollfd, int efd, int max) {
  epoll_event *events = (struct epoll_event *)calloc(max, sizeof(epoll_event));

  int ctr = 0;
  while (ctr < max) {
    int n = epoll_wait(epollfd, events, max, -1);
    for (int i = 0; i < n; i++) {
      if (efd == events[i].data.fd) {
        gIOStatus iostatus;
        int ret = read(efd, &iostatus, sizeof(iostatus));
        if (ret == sizeof(iostatus)) {
          VLOG(1) << "Recieved event (completionId: " << iostatus.completionId
                  << " , status: " << iostatus.errorCode;
          ctr++;
        }
      }
    }
    struct epoll_event event;
    event.data.fd = efd;
    event.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
    int s = epoll_ctl(epollfd, EPOLL_CTL_MOD, efd, &event);
    assert(s == 0);
  }

  free(events);
  return 0;
}

static void run_verifyIO() {
  int epollfd = epoll_create1(0);
  int ret = 0;
  assert(epollfd >= 0);

  gMempool_init(ALIGNSIZE);

  // figure out how to provide the path to config file in a test
  auto serviceHandle = IOExecFileServiceInit("../../../src/gioexecfile.conf");

  IOExecFileHandle handle;
  handle = IOExecFileOpen(serviceHandle, "/tmp/abc", O_RDWR | O_CREAT);
  if (handle == nullptr) {
    LOG(ERROR) << "IOExecFileOpen Failed";
    return;
  }

  auto evHandle = IOExecEventFdOpen(serviceHandle);
  if (evHandle == nullptr) {
    LOG(ERROR) << "getEventFd failed";
    return;
  }

  int efd = IOExecEventFdGetReadFd(evHandle);
  epoll_event event;
  event.data.fd = efd;
  event.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
  int s = epoll_ctl(epollfd, EPOLL_CTL_ADD, efd, &event);
  assert(s == 0);

  gIOBatch *write_batch = gIOBatchAlloc(NUMIO);

  off_t offset = 0;
  for (int i = 0; i < NUMIO; i++) {
    gIOExecFragment &frag = write_batch->array[i];
    frag.addr = (caddr_t)gMempool_alloc(BLOCKSIZE);
    assert(frag.addr != nullptr);
    memset(frag.addr, 'a' + i, BLOCKSIZE);
    frag.offset = offset;
    frag.size = BLOCKSIZE;
    frag.completionId = i;
    offset += BLOCKSIZE;
  }
  ret = IOExecFileWrite(handle, write_batch, evHandle);
  assert(ret == 0);

  wait_for_iocompletion(epollfd, efd, NUMIO);

  gIOBatch *read_batch = gIOBatchAlloc(NUMIO);

  offset = 0;
  for (int i = 0; i < NUMIO; i++) {
    gIOExecFragment &frag = read_batch->array[i];
    frag.addr = (caddr_t)gMempool_alloc(BLOCKSIZE);
    assert(frag.addr != nullptr);
    memset(frag.addr, 'a' + i, BLOCKSIZE);
    frag.offset = offset;
    frag.size = BLOCKSIZE;
    frag.completionId = i;
    offset += BLOCKSIZE;
  }
  ret = IOExecFileRead(handle, read_batch, evHandle);
  assert(ret == 0);

  wait_for_iocompletion(epollfd, efd, NUMIO);

  for (int i = 0; i < NUMIO; i++) {
    gIOExecFragment &read_frag = read_batch->array[i];
    gIOExecFragment &write_frag = write_batch->array[i];
    EXPECT_TRUE(memcmp(read_frag.addr, write_frag.addr, BLOCKSIZE) == 0);
    VLOG(1) << "idx=" << i << ":writebuf=" << std::string(write_frag.addr, 10);
    VLOG(1) << "idx=" << i << ":readbuf=" << std::string(read_frag.addr, 10);
  }

  IOExecFileClose(handle);

  IOExecEventFdClose(evHandle);

  IOExecFileServiceDestroy(serviceHandle);
}

TEST(IOExecFile, OneFileReadWrite) 
{ 
  run_verifyIO(); 
}

