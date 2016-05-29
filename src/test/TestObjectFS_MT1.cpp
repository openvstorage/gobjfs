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

#include <TestObjectFS_MT1.h>

/*
 * Wait for read/write I/Os to complete
 */
static int wait_for_iocompletion(int epollfd, int efd, int max, bool writeop) {
  epoll_event *events = (struct epoll_event *)calloc(max, sizeof(epoll_event));

  int ctr = 0;
  while (ctr < max) {
    // LEVELTRIGGERED is default
    // Edge triggerd
    int n = epoll_wait(epollfd, events, max, -1);
    for (int i = 0; i < n; i++) {
      if (efd != events[i].data.fd) {
        continue;
      }

      gIOStatus iostatus;
      int ret = read(efd, &iostatus, sizeof(iostatus));

      if (ret != sizeof(iostatus)) {
        LOG(ERROR) << "Invalid IO Status of size: " << ret;
        continue;
      }

      VLOG(2) << "Recieved event"
              << " (completionId: " << iostatus.completionId
              << " status: " << iostatus.errorCode;

      ctr++;

      if (writeop) {
        ObjectInfo *obj = (ObjectInfo *)iostatus.completionId;
        assert(obj != nullptr);
        VLOG(2) << ctr << ") "
                << "ObjectID: " << obj->meta.objectId << " Written to:"
                << " Container: " << obj->meta.containerId
                << " Segment: " << obj->meta.segmentId;
        ObjectList.add(obj);

      } else {
        RdStatusExt *statusExt = (RdStatusExt *)iostatus.completionId;
        assert(statusExt != nullptr);
        gRdStatus *status = &statusExt->rdStatus;
        gRdObject *rdObject = statusExt->rdObject;
        assert(status->errorStatus == 0);

        VLOG(2) << "Buf: " << rdObject->buf[0]
                << " BufExpected: " << statusExt->expectedBuf[0];
        EXPECT_TRUE(memcmp(rdObject->buf, statusExt->expectedBuf, OBJECTSZ) ==
                    0);
        VLOG(2) << ctr << ")" << status->completionId
                << " Read completed Successfully";
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
void writerhread(void *arg) {
  WrThreadCtx *ctx = (WrThreadCtx *)arg;

  int epollfd = epoll_create1(0);
  int ret = 0;
  assert(epollfd >= 0);

  VLOG(0) << "Writer Thread Started";

  ObjectFSHandle_t handle;

  if (ObjectFS_Open(TESTFILE, O_WRONLY, &handle) < 0) {
    LOG(ERROR) << "OpenFS failed to open " << TESTFILE;
    return;
  }

  int efd = -1;
  if (ObjectFS_GetEventFd(handle, &efd) < 0) {
    LOG(ERROR) << "getEventFd failed";
    ObjectFS_Close(&handle);
    return;
  }

  struct epoll_event event;
  event.data.fd = efd;
  event.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
  int s = epoll_ctl(epollfd, EPOLL_CTL_ADD, efd, &event);
  assert(s == 0);

  assert(ctx->count > 0 && ctx->objectCacheSz > 0);
  assert(ctx->batchSz > 0 && ctx->count > ctx->batchSz &&
         (ctx->count % ctx->batchSz == 0));

  Object *objectCache = ctx->objectCache;
  ObjectInfo *objectInfo = ctx->objectInfo;
  gWrObject *wrObject = (gWrObject *)malloc(sizeof(gWrObject) * ctx->batchSz);

  assert(objectCache != nullptr);
  assert(objectInfo != nullptr);
  assert(wrObject != nullptr);

  VLOG(2) << "Writing Objects to " << TESTFILE;

  /*
   * objectId - Unique ID generated from 0 to ctx->count
   * completionId - Points to objectInfo - size of number of IOs
   * buf - pointing to data in objectCache
   */
  for (int i = 0; i < ctx->count; i += ctx->batchSz) {
    int objectCacheIdx = 0;
    for (int j = 0; j < (ctx->batchSz); j++) {
      int idx = i + j;

      objectInfo[idx].data = objectCache[objectCacheIdx].data;

      wrObject[j].objectId = idx;
      wrObject[j].completionId = (gCompletionID) & (objectInfo[idx]);
      wrObject[j].buf = objectCache[objectCacheIdx].data;
      wrObject[j].len = OBJECTSZ;

      do {
        ret = ObjectFS_Put(handle, &wrObject[j], &(objectInfo[idx].meta));
        VLOG(2) << "Write Buffer[idx=" << idx << " ] sent. status:  " << ret;
      } while (ret == -EAGAIN);
      assert(ret == 0);

      objectCacheIdx++;
      if (objectCacheIdx > ctx->objectCacheSz - 1) {
        objectCacheIdx = 0;
      }
    }
    wait_for_iocompletion(epollfd, efd, ctx->batchSz, WRITEOP);
  }

  ObjectFS_Close(&handle);

  VLOG(2) << "Written " << ctx->count << " Objects";
  VLOG(0) << "Writer Thread Exit";
}

/*
 * Reads #NUMREADS per thread from ObjectList
 */
static void reader_thread(void *arg) {

  VLOG(0) << "Reader thread started";

  assert(arg != nullptr);
  RdThreadCtx *ctx = (RdThreadCtx *)arg;

  int epollfd = epoll_create1(0);
  int ret = 0;
  assert(epollfd >= 0);

  ObjectFSHandle_t handle;

  if (ObjectFS_Open(TESTFILE, O_RDONLY, &handle) < 0) {
    LOG(ERROR) << "OpenFS failed to open " << TESTFILE;
    return;
  }

  int efd = -1;
  if (ObjectFS_GetEventFd(handle, &efd) < 0) {
    LOG(ERROR) << "getEventFd failed";
    ObjectFS_Close(&handle);
    return;
  }

  struct epoll_event event;
  event.data.fd = efd;
  event.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
  int s = epoll_ctl(epollfd, EPOLL_CTL_ADD, efd, &event);
  assert(s == 0);

  assert(ctx->count > 0);
  assert(ctx->batchSz > 0 && ctx->count > ctx->batchSz &&
         (ctx->count % ctx->batchSz == 0));

  Object *objectCache = (Object *)malloc(sizeof(Object) * ctx->batchSz);
  for (int i = 0; i < ctx->batchSz; i++) {
    ret = posix_memalign((void **)&objectCache[i].data, ALIGNSZ, OBJECTSZ);
    assert(ret == 0);
  }
  gRdObject *rdObject = (gRdObject *)malloc(sizeof(gRdObject) * ctx->batchSz);
  RdStatusExt *rdStatusExt =
      (RdStatusExt *)malloc(sizeof(RdStatusExt) * ctx->batchSz);

  assert(objectCache != nullptr);
  assert(rdObject != nullptr);
  assert(rdStatusExt != nullptr);

  for (int i = 0; i < ctx->count; i += ctx->batchSz) {
    for (int j = 0; j < ctx->batchSz; j++) {
      ObjectInfo *obj = ObjectList.getRandomObject(); /* Blocking call */
      assert(obj != nullptr);

      rdObject[j].buf = objectCache[j].data;
      rdObject[j].len = OBJECTSZ;
      rdObject[j].containerId = obj->meta.containerId;
      // rdObject[j].segmentId = obj->meta.segmentId;
      rdStatusExt[j].rdObject = &(rdObject[j]);
      rdStatusExt[j].expectedBuf = obj->data;
      VLOG(2) << "obj->data " << obj->data[0] << " , Read ExpectBuf set to "
              << rdStatusExt[j].expectedBuf[0];
      rdObject[j].completionId = (gCompletionID) & (rdStatusExt[j]);

      do {
        ret = ObjectFS_Get(handle, &rdObject[j], &(rdStatusExt[j].rdStatus), 1);
        VLOG(2) << "Read Buffer[idx=" << j << " ] sent. status:  " << ret;
      } while (ret == -EAGAIN);
      assert(ret == 0);
    }
    wait_for_iocompletion(epollfd, efd, ctx->batchSz, READOP);
  }

  free(objectCache);
  free(rdObject);
  free(rdStatusExt);

  ObjectFS_Close(&handle);
  VLOG(0) << "Reader Thread Exit";
}

/*
 * MultiThreaded_OneDeviceReadWrite:
 *
 * - One writer thread, which writes fixed size Objects  in batch and waits for
 * I/O to complete
 * - N reader threads, which reads randaom objects written by the test &
 * compares data
 */
TEST(ObjectFS, MultiThreaded_OneDeviceReadWrite1) {
  if (ObjectFS_Init(IOCONFIGFILE) < 0) {
    LOG(ERROR) << "ObjectFS_init Failed";
    return;
  }

  std::vector<std::thread> threads;

  /*
   * Initialize global object cache which is used by write
   * & by read to compare data
   */
  Object *objectCache = (Object *)malloc(WROBJECT_CACHESZ * sizeof(Object));
  assert(objectCache != nullptr);
  for (int i = 0; i < WROBJECT_CACHESZ; i++) {
    int ret = posix_memalign((void **)&objectCache[i].data, ALIGNSZ, OBJECTSZ);
    assert(ret == 0);
    memset(objectCache[i].data, 'a' + i, OBJECTSZ);
  }
  wrThreadCtx.objectInfo =
      (ObjectInfo *)malloc(sizeof(ObjectInfo) * NUMOBJECTS);
  assert(wrThreadCtx.objectInfo != nullptr);

  wrThreadCtx.objectCache = objectCache;
  wrThreadCtx.objectCacheSz = WROBJECT_CACHESZ;
  wrThreadCtx.count = NUMOBJECTS;
  wrThreadCtx.batchSz = WRBATCHSZ;

  threads.push_back(std::thread(writerhread, (void *)&wrThreadCtx));

  for (int i = 0; i < RDTHREADS; i++) {
    rdThreadCtx[i].count = (NUMREADS / RDTHREADS);
    rdThreadCtx[i].batchSz = RDBATCHSZ;
    threads.push_back(std::thread(reader_thread, (void *)&rdThreadCtx[i]));
  }

  for (auto it = threads.begin(); it != threads.end(); it++) {
    it->join();
  }

  for (int i = 0; i < WROBJECT_CACHESZ; i++) {
    free(objectCache[i].data);
  }
  free(objectCache);
  free(wrThreadCtx.objectInfo);

  ObjectFS_Destroy();
  VLOG(0) << "MultiThreaded_OneDeviceReadWrite1 Ext !!";
}
