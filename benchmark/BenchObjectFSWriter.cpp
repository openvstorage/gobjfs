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

#include <assert.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <gobj.h>
#include <gtest/gtest.h>
#include <iostream>
#include <math.h>
#include <stdint.h> /* Int types */
#include <string.h>
#include <sys/epoll.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <unistd.h>
#include <vector>

#include <boost/program_options.hpp>
#include <condition_variable>
#include <fstream>
#include <mutex>
#include <thread>

using namespace boost::program_options;

struct Config {
  size_t objectSz = 16384;
  size_t alignSz = 4096;
  size_t objectCacheSz = 1000;

  int32_t totalIOps = 1000000;
  int32_t maxWriterThreads = 1;
  size_t containerSz = 1073741824;
  size_t containerMetaSz = 4096;
  size_t segmentSz = 16384;
  std::string ioConfigFile = {"/etc/gobjfs/TestObjectFS.conf"};
  std::string device = {"/dev/loop2"};

  Config() {}

  void readConfig(const std::string &configFileName) {
    options_description desc("allowed options");
    desc.add_options()("object_sz", value<size_t>(&objectSz)->required(),
                       "obejct size for reads & writes")(
        "align_sz", value<size_t>(&alignSz)->required(),
        "size that memory is aligned to ")(
        "object_cache_sz", value<size_t>(&objectCacheSz)->required(),
        "Object Cache Size - buffered write objects")(
        "total_iops", value<int32_t>(&totalIOps)->required(), "Number of IOPs")(
        "max_writer_threads", value<int32_t>(&maxWriterThreads)->required(),
        "Number of Writer Threads")(
        "container_sz", value<size_t>(&containerSz)->required(),
        "Containers of size disk space divided into")(
        "container_meta_sz", value<size_t>(&containerMetaSz)->required(),
        "Container Metadata Size")(
        "segment_sz", value<size_t>(&segmentSz)->required(),
        "Segments of size a container space divided into")(
        "ioconfig_file", value<std::string>(&ioConfigFile)->required(),
        "IOExecutor config file")(
        "device", value<std::string>(&device)->required(), "Device file path");
    std::ifstream configFile(configFileName);
    variables_map vm;
    store(parse_config_file(configFile, desc), vm);
    notify(vm);

    LOG(INFO)
        << "================================================================="
        << std::endl << " 		BenchObjectFSWriter.conf" << std::endl
        << "================================================================="
        << std::endl;
    std::ostringstream s;
    for (const auto &it : vm) {
      s << it.first.c_str() << "=";
      auto &value = it.second.value();
      if (auto v = boost::any_cast<size_t>(&value))
        s << *v << std::endl;
      else if (auto v = boost::any_cast<int32_t>(&value))
        s << *v << std::endl;
      else if (auto v = boost::any_cast<std::string>(&value))
        s << *v << std::endl;
      else if (auto v = boost::any_cast<bool>(&value))
        s << *v << std::endl;
      else
        s << "error" << std::endl;
    }
    LOG(INFO) << s.str();
    LOG(INFO)
        << "=================================================================="
        << std::endl;
  }
};

/*
 * All written Objects are maintained
 * under ObjectInfo
 */
struct ObjectInfo {
  gObjectMetadata meta;
  char *data;
};

struct Object {
  char *data;
};

/*
 * Write thread context
 */
struct WrThreadCtx {
  std::string device;
  ObjectInfo *objectInfo;
  Object *objectCache; /* Array of WROBJECT_CACHE_SZ */
  int count;
  int batchSz;
  int objectSz;
  int objectCacheSz;
};

WrThreadCtx wrThreadCtx;

/*
 * Wait for read/write I/Os to complete
 */
static int wait_for_iocompletion(int epollfd, int efd, int max) {
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

      ObjectInfo *obj = (ObjectInfo *)iostatus.completionId;
      assert(obj != nullptr);
      VLOG(2) << ctr << ") "
              << "ObjectID: " << obj->meta.objectId << " Written to:"
              << " Container: " << obj->meta.containerId
              << " Segment: " << obj->meta.segmentId;
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
void writer_thread(void *arg) {
  WrThreadCtx *ctx = (WrThreadCtx *)arg;

  int epollfd = epoll_create1(0);
  int ret = 0;
  assert(epollfd >= 0);

  VLOG(0) << "Writer Thread Started" << ctx->count;

  ObjectFSHandle_t handle;

  if (ObjectFS_Open(ctx->device.c_str(), O_WRONLY, &handle) < 0) {
    LOG(ERROR) << "OpenFS failed to open " << ctx->device;
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

  Object *objectCache = ctx->objectCache;
  gWrObject *wrObject = (gWrObject *)malloc(sizeof(gWrObject) * ctx->count);
  ObjectInfo *objectInfo =
      (ObjectInfo *)malloc(sizeof(ObjectInfo) * ctx->count);

  assert(objectCache != nullptr);
  assert(objectInfo != nullptr);
  assert(wrObject != nullptr);

  std::thread ioCompletionThread(wait_for_iocompletion, epollfd, efd,
                                 ctx->count);

  /*
   * objectId - Unique ID generated from 0 to ctx->count
   * completionId - Points to objectInfo - size of number of IOs
   * buf - pointing to data in objectCache
   */
  int objectCacheIdx = 0;
  for (int i = 0; i < ctx->count; i++) {

    objectInfo[i].data = objectCache[objectCacheIdx].data;

    wrObject[i].objectId = i;
    wrObject[i].completionId = (gCompletionID) & (objectInfo[i]);
    wrObject[i].buf = objectCache[objectCacheIdx].data;
    wrObject[i].len = ctx->objectSz;

    do {
      ret = ObjectFS_Put(handle, &wrObject[i], &(objectInfo[i].meta));
      VLOG(2) << "Write Buffer[idx=" << i << " ] sent. status:  " << ret;
    } while (ret == -EAGAIN);
    assert(ret == 0);

    objectCacheIdx++;
    if (objectCacheIdx > ctx->objectCacheSz - 1) {
      objectCacheIdx = 0;
    }
  }

  ioCompletionThread.join();
  ObjectFS_Close(&handle);

  VLOG(0) << "Written " << ctx->count << " Objects";
  VLOG(0) << "Writer Thread Exit";
}

/*
 * MultiThreaded_OneDeviceReadWrite:
 *
 * - One writer thread, which writes fixed size Objects  in batch and waits for
 * I/O to complete
 * - N reader threads, which reads randaom objects written by the test &
 * compares data
 */
int main(int argc, char *argv[]) {
  Config config;
  if (argc == 2) {
    config.readConfig(argv[1]);
  }
  // Config config;

  // TODO Read COnfig file

  if (ObjectFS_Init(config.ioConfigFile.c_str()) < 0) {
    LOG(ERROR) << "ObjectFS_init Failed";
    return -1;
  }

  std::vector<std::thread> threads;
  WrThreadCtx *wrThreadCtx =
      (WrThreadCtx *)malloc(sizeof(WrThreadCtx) * config.maxWriterThreads);
  /*
   * Initialize global object cache which is used by write
   * & by read to compare data
   */

  Object *objectCache = (Object *)malloc(config.objectCacheSz * sizeof(Object));
  assert(objectCache != nullptr);
  for (size_t i = 0; i < config.objectCacheSz; i++) {
    int ret = posix_memalign((void **)&objectCache[i].data, config.alignSz,
                             config.objectSz);
    assert(ret == 0);
    memset(objectCache[i].data, 'a' + i, config.objectSz);
  }

  for (int i = 0; i < config.maxWriterThreads; i++) {
    int countPerThread =
        ceil((double)config.totalIOps / (double)config.maxWriterThreads);

    wrThreadCtx[i].device = config.device;
    wrThreadCtx[i].objectCache = objectCache;
    wrThreadCtx[i].objectCacheSz = config.objectCacheSz;
    wrThreadCtx[i].objectSz = config.objectSz;
    wrThreadCtx[i].count = countPerThread;
    threads.push_back(std::thread(writer_thread, (void *)&wrThreadCtx[i]));
  }

  for (auto it = threads.begin(); it != threads.end(); it++) {
    it->join();
  }

  free(objectCache);
  free(wrThreadCtx);

  ObjectFS_Destroy();
  VLOG(0) << "MultiThreaded_OneDeviceReadWrite1 Ext !!";
  return 0;
}
