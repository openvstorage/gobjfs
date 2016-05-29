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

#include <fcntl.h>
#include <glog/logging.h>
#include <gobj.h>
#include <gtest/gtest.h>
#include <stdint.h> /* Int types */
#include <sys/epoll.h>
#include <sys/types.h>
#include <unistd.h>
#include <unistd.h>

#define TESTFILE "/dev/loop2"
#define IOCONFIGFILE "/home/vikram/gobjfs/tmp/gobjfs/src/test/TestObjectFS.conf"

/*
 * TEST: verifyIO
 *  - Write Data to file
 *  - Make sure data is written, by re-reading it // TODO
 */
#define BLOCKSIZE 16384
#define ALIGNSIZE 4096
#define NUMIO 100

int writing_data = -1;

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
          VLOG(0) << "Recieved event (completionId: " << iostatus.completionId
                  << " , status: " << iostatus.errorCode;
          // TODO: Based on completion ID identify type of requires
          ctr++;
          if (writing_data == 1) {
            gObjectMetadata_t *meta =
                (gObjectMetadata_t *)iostatus.completionId;
            VLOG(0) << ctr << ") "
                    << "ObjectID: " << meta->objectId << " Written to:"
                    << " Container: " << meta->containerId
                    << " Segment: " << meta->segmentId;
          } else if (!writing_data) {
            gRdStatus_t *status = (gRdStatus_t *)iostatus.completionId;
            VLOG(0) << ctr << ") "
                    << "CompletionID: " << status->completionId << " Read !!";
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

static void run_verifyIO() {
  int epollfd = epoll_create1(0);
  int ret = 0;
  assert(epollfd >= 0);

  ObjectFSHandle_t handle;
  // std::string   fileName(TESTFILE);

  if (ObjectFS_Init(IOCONFIGFILE) < 0) {
    VLOG(0) << "ObjectFS_init Failed";
    return;
  }

  if (ObjectFS_Open(TESTFILE, O_RDWR, &handle) < 0) {
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

  gOffset offset = 0;
  gWrObject_t *wrObject = (gWrObject_t *)malloc(sizeof(gWrObject_t) * NUMIO);
  assert(wrObject != NULL);
  gObjectMetadata_t *objectMetadata =
      (gObjectMetadata_t *)malloc(sizeof(gObjectMetadata_t) * NUMIO);
  assert(objectMetadata != NULL);

  writing_data = 1;
  VLOG(0) << "Writing buffers to " << TESTFILE;
  for (int i = 0; i < NUMIO; i++) {

    ret = posix_memalign((void **)&wrObject[i].buf, ALIGNSIZE, BLOCKSIZE);
    assert(ret == 0);
    wrObject[i].objectId = i;
    wrObject[i].completionId = (gCompletionID) & (objectMetadata[i]);
    wrObject[i].len = BLOCKSIZE;
    memset(wrObject[i].buf, 'a' + i, BLOCKSIZE);

    ret = ObjectFS_Put(handle, &wrObject[i], &(objectMetadata[i]));
    VLOG(0) << "Write Buffer[ " << i << " ]: " << ret;
    assert(ret == 0);
    offset += BLOCKSIZE;
  }

  wait_for_iocompletion(epollfd, efd, NUMIO);

  writing_data = 0;
  gRdObject_t *rdObject = (gRdObject_t *)malloc(sizeof(gRdObject_t) * NUMIO);
  assert(rdObject != NULL);
  gRdStatus_t *rdStatus = (gRdStatus_t *)malloc(sizeof(gRdStatus_t) * NUMIO);
  assert(rdStatus != NULL);

  for (int i = 0; i < NUMIO; i++) {

    ret = posix_memalign((void **)&rdObject[i].buf, ALIGNSIZE, BLOCKSIZE);
    assert(ret == 0);
    rdObject[i].len = BLOCKSIZE;
    rdObject[i].containerId = objectMetadata[i].containerId;
    // rdObject[i].segmentId = objectMetadata[i].segmentId;
    rdObject[i].completionId = (gCompletionID) & (rdStatus[i]);

    ret = ObjectFS_Get(handle, &rdObject[i], &rdStatus[i], 1);
    VLOG(0) << "Read Buffer[idx=" << i << " ] sent. status:  " << ret;
    assert(ret == 0);
    // offset += BLOCKSIZE;
  }

  wait_for_iocompletion(epollfd, efd, NUMIO);

  for (int i = 0; i < NUMIO; i++) {
    EXPECT_TRUE(memcmp(rdObject[i].buf, wrObject[i].buf, BLOCKSIZE) == 0);
    free(rdObject[i].buf);
    free(wrObject[i].buf);
  }

  free(wrObject);
  ObjectFS_Close(&handle);
  ObjectFS_Destroy();
}

TEST(ObjectFS, OneDeviceReadWrite) {
  // int fd = open(TESTFILE, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR |
  // S_IROTH | S_IRGRP | S_IWGRP);
  int capture_errno = errno;
  // assert(fd >= 0);

  run_verifyIO();
  int ret = ::unlink(TESTFILE);
  assert(ret == 0);
}
