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

#include <condition_variable>
#include <fcntl.h>
#include <glog/logging.h>
#include <gobj.h>
#include <gtest/gtest.h>
#include <mutex>
#include <stdint.h> /* Int types */
#include <sys/epoll.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <unistd.h>

#define TESTFILE "/dev/loop2"
#define IOCONFIGFILE "/etc/gobjfs/TestObjectFS.conf"

#define BLOCKSZ 16384 /* Length of object is fixed */
#define ALIGNSZ 4096

#define WRTHREADS 1
#define RDTHREADS 1

#define NUMOBJECTS 100 /* Total Number of Objects written */
#define WRBATCHSZ 10
#define NUMREADS 100 /* Reads per thread */

/*
 *  Class that holds objects written on ObjectFs
 *  It's used by reader threads to randomly pick random
 *  Objects and read them from disk
 */
class WrittenObjects {
private:
  std::vector<gObjectMetadata_t *> objVec_;
  std::mutex lock_;
  std::condition_variable cond_;

public:
  WrittenObjects() {}

  ~WrittenObjects() {
    for (auto it = objVec_.begin(); it != objVec_.end(); it++) {
      gObjectMetadata_t *meta = *it;
      free(meta);
    }
  }

  void add(gObjectMetadata_t *meta) {
    {
      std::unique_lock<std::mutex> lck(lock_);
      objVec_.push_back(meta);
      cond_.notify_one();
    }
  }

  gObjectMetadata_t *getRandomObject() {
    gObjectMetadata_t *meta = nullptr;
    {
      std::unique_lock<std::mutex> lck(lock_);
      cond_.wait(lck, [this] { return (!this->objVec_.empty()); });

      std::vector<gObjectMetadata_t *>::iterator randIt = objVec_.begin();
      std::advance(randIt, std::rand() % objVec_.size());
      meta = *randIt;
    }
    return meta;
  }
};

/*
 * Need to keep track Rd/Write buffers
 * to user/free on async I/O completion
 */
class CompletionIdMap {
private:
  std::map<gCompletionID, char *> map_;
  std::mutex lock_;

public:
  CompletionIdMap(){};
  ~CompletionIdMap(){};
  void add(gCompletionID id, char *val) {
    {
      std::unique_lock<std::mutex> lck(lock_);
      map_.emplace(id, val);
    }
  }

  char *rem(gCompletionID id) {
    char *addr;
    {
      std::unique_lock<std::mutex> lck(lock_);
      auto result = map_.find(id);
      addr = result->second;
      map_.erase(id);
    }
    return addr;
  }
};

struct ThreadCtx {
  gRdObject_t *rdObject;
  gRdStatus_t *rdStatus;
  int count;
};

CompletionIdMap completionIdMap;
WrittenObjects writtenObjects;               /* Tracks written objects */
gObjectMetadata_t *objectMetadata = nullptr; /* Written Objects Array */
gWrObject_t *wrObject = nullptr;
gRdObject_t *rdObject = nullptr;
gRdStatus_t *rdStatus = nullptr;
ThreadCtx readThreadCtx[RDTHREADS];

/*
 * Wait for read/write I/Os to complete
 */
static int wait_for_iocompletion(int epollfd, int efd, int max, bool writeop) {
  epoll_event *events = (struct epoll_event *)calloc(max, sizeof(epoll_event));

  int ctr = 0;
  while (ctr < max) {
    int n = epoll_wait(epollfd, events, max, -1);
    for (int i = 0; i < n; i++) {
      if (efd == events[i].data.fd) {
        gIOStatus iostatus;
        int ret = read(efd, &iostatus, sizeof(iostatus));

        if (ret == sizeof(iostatus)) {
          VLOG(0) << "Recieved event (completionId: " << iostatus.completionId
                  << " , status: " << iostatus.errorCode;
          ctr++;

          if (writeop) {
            gWrObject_t *wrObj =
                (gWrObject_t *)completionIdMap.rem(iostatus.completionId);
            assert(wrObj != NULL);
            free(wrObj->buf);
            free(wrObj);

            gObjectMetadata_t *meta =
                (gObjectMetadata_t *)iostatus.completionId;
            VLOG(0) << ctr << ") "
                    << "ObjectID: " << meta->objectId << " Written to:"
                    << " Container: " << meta->containerId
                    << " Segment: " << meta->segmentId;
            writtenObjects.add(meta);

          } else {
            gRdObject_t *rdObj =
                (gRdObject_t *)completionIdMap.rem(iostatus.completionId);
            assert(rdObj != NULL);
            // TODO compare data that is written & read
            free(rdObj->buf);
            free(rdObj);

            gRdStatus_t *status = (gRdStatus_t *)iostatus.completionId;
            VLOG(0) << ctr << ") "
                    << "CompletionID: " << status->completionId << " Read !!";
            free(status);
          }
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

/*
 * Writes #NUMOBJECTS objects in batches of #WRBATCHSZ
 */
void writer_thread() {
  int epollfd = epoll_create1(0);
  int ret = 0;
  assert(epollfd >= 0);

  VLOG(0) << "Writer Thread Invoked";

  ObjectFSHandle_t handle;

  if (ObjectFS_Open(TESTFILE, O_WRONLY, &handle) < 0) {
    VLOG(0) << "OpenFS failed to open " << TESTFILE;
    return;
  }

  int efd = -1;
  if (ObjectFS_GetEventFd(handle, &efd) < 0) {
    VLOG(0) << "getEventFd failed";
    ObjectFS_Close(&handle);
    return;
  }

  struct epoll_event event;
  event.data.fd = efd;
  event.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
  int s = epoll_ctl(epollfd, EPOLL_CTL_ADD, efd, &event);
  assert(s == 0);

  gWrObject_t *wrObject =
      (gWrObject_t *)malloc(sizeof(gWrObject_t) * NUMOBJECTS);
  assert(wrObject != NULL);
  objectMetadata =
      (gObjectMetadata_t *)malloc(sizeof(gObjectMetadata_t) * NUMOBJECTS);
  assert(objectMetadata != NULL);

  VLOG(0) << "Writing Objects to " << TESTFILE;

  for (int i = 0; i < NUMOBJECTS; i += WRBATCHSZ) {

    for (int j = 0; j < WRBATCHSZ; j++) {
      ret = posix_memalign((void **)&wrObject[i + j].buf, ALIGNSZ, BLOCKSZ);
      assert(ret == 0);

      wrObject[i + j].objectId = i + j;
      wrObject[i + j].completionId = (gCompletionID) & (objectMetadata[i + j]);
      wrObject[i + j].len = BLOCKSZ;
      memset(wrObject[i + j].buf, 'a' + j, BLOCKSZ);

      completionIdMap.add(wrObject[i + j].completionId,
                          (char *)&wrObject[i + j]);
      assert(wrObject[i + j].completionId != 0);

      ret = ObjectFS_Put(handle, &wrObject[i + j], &(objectMetadata[i + j]));
      VLOG(0) << "Write Buffer[ " << i + j << " ]: " << ret;
      assert(ret == 0);
    }
    wait_for_iocompletion(epollfd, efd, WRBATCHSZ, 1);
  }

  ObjectFS_Close(&handle);

  VLOG(0) << "Written " << NUMOBJECTS << " Objects";
}

/*
 * Reads #NUMREADS per thread from writtenObjects
 */
static void reader_thread(void *arg) {

  VLOG(0) << "Reader thread invoked";

  assert(arg != NULL);
  ThreadCtx *ctx = (ThreadCtx *)arg;

  int epollfd = epoll_create1(0);
  int ret = 0;
  assert(epollfd >= 0);

  ObjectFSHandle_t handle;

  if (ObjectFS_Open(TESTFILE, O_RDONLY, &handle) < 0) {
    VLOG(0) << "OpenFS failed to open " << TESTFILE;
    ObjectFS_Destroy();
    return;
  }

  int efd = -1;
  if (ObjectFS_GetEventFd(handle, &efd) < 0) {
    VLOG(0) << "getEventFd failed";
    ObjectFS_Close(&handle);
    ObjectFS_Destroy();
    return;
  }

  struct epoll_event event;
  event.data.fd = efd;
  event.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
  int s = epoll_ctl(epollfd, EPOLL_CTL_ADD, efd, &event);
  assert(s == 0);

  gRdObject_t *rdObject = ctx->rdObject;
  gRdStatus_t *rdStatus = ctx->rdStatus;
  assert(rdObject != NULL);
  assert(rdStatus != NULL);

  for (int i = 0; i < NUMREADS / RDTHREADS; i++) {

    gObjectMetadata_t *meta =
        writtenObjects.getRandomObject(); /* Blocking call */
    assert(meta != NULL);

    ret = posix_memalign((void **)&rdObject[i].buf, ALIGNSZ, BLOCKSZ);
    assert(ret == 0);
    rdObject[i].len = BLOCKSZ;
    rdObject[i].containerId = meta->containerId;
    // rdObject[i].segmentId = meta->segmentId;
    rdObject[i].completionId = (gCompletionID) & (rdStatus[i]);
    assert(rdObject[i].completionId != 0);
    completionIdMap.add(rdObject[i].completionId, (char *)&rdObject[i]);

    ret = ObjectFS_Get(handle, &rdObject[i], &rdStatus[i], 1);
    VLOG(0) << "Read Buffer[idx=" << i << " ] sent. status:  " << ret;
    assert(ret == 0);
  }

  wait_for_iocompletion(epollfd, efd, NUMREADS, 0);

  ObjectFS_Close(&handle);
}

/*
 * MultiThreaded_OneDeviceReadWrite:
 *
 * - One writer thread, which writes fixed size Objects  in batch and waits for
 * I/O to complete
 * - N reader threads, which reads randaom objects written by the test &
 * compares data
 */
TEST(ObjectFS, MultiThreaded_OneDeviceReadWrite) {
  if (ObjectFS_Init(IOCONFIGFILE) < 0) {
    VLOG(0) << "ObjectFS_init Failed";
    return;
  }

  std::vector<std::thread> threads;

  threads.push_back(std::thread(writer_thread));

  for (int i = 0; i < RDTHREADS; i++) {

    readThreadCtx[i].rdStatus =
        (gRdStatus_t *)malloc(sizeof(gRdStatus_t) * (NUMREADS / RDTHREADS));
    assert(readThreadCtx[i].rdStatus != NULL);

    readThreadCtx[i].rdObject =
        (gRdObject_t *)malloc(sizeof(gRdObject_t) * (NUMREADS / RDTHREADS));
    assert(readThreadCtx[i].rdObject != NULL);

    threads.push_back(std::thread(reader_thread, (void *)&readThreadCtx[i]));
  }

  for (auto it = threads.begin(); it != threads.end(); it++) {
    it->join();
  }

  for (int i = 0; i < RDTHREADS; i++) {
    free(readThreadCtx[i].rdStatus);
    free(readThreadCtx[i].rdObject);
  }

  ObjectFS_Destroy();
}
