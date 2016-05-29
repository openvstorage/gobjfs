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
#include <sys/epoll.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <unistd.h>

#include <assert.h>
#include <chrono>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <iostream>
#include <math.h>
#include <string.h>
#include <sys/time.h>
#include <vector>

#include "../util/Stats.h"
#include "../util/Timer.h"
#include <ObjectFS.h>
#include <atomic>
#include <boost/program_options.hpp>
#include <condition_variable>
#include <fstream>
#include <gobj.h>
#include <mutex>
#include <strings.h>
#include <thread>

#define MEMCHECK 1
using namespace boost::program_options;
using namespace std;

struct Config {
  size_t objectSz = 16384;
  size_t alignSz = 4096;
  size_t objectCacheSz = 1000;

  int32_t totalWrIOps = 10000;
  int32_t totalRdIOps = 10000;
  int32_t fillData = 0;
  int32_t maxWriterThreads = 1;
  int32_t maxReaderThreads = 1;
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
        "total_read_iops", value<int32_t>(&totalRdIOps)->required(),
        "Number of Read IOPs")("total_write_iops",
                               value<int32_t>(&totalWrIOps)->required(),
                               "Number of Write IOPs")(
        "fill_data", value<int32_t>(&fillData)->required(),
        "Only for read operations, fill specified number of data")(
        "max_writer_threads", value<int32_t>(&maxWriterThreads)->required(),
        "Number of Writer Threads")(
        "max_reader_threads", value<int32_t>(&maxReaderThreads)->required(),
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
        << std::endl << "     BenchObjectFSWriter.conf" << std::endl
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

class ObjectList {
private:
  std::vector<ObjectInfo *> objVec_;
  std::mutex lock_;
  std::condition_variable cond_;

public:
  ObjectList() {}

  ~ObjectList() { objVec_.erase(objVec_.begin(), objVec_.end()); }

  void add(ObjectInfo *obj) {
    {
      // No locks - writer must finish before reader in bench
      // std::unique_lock<std::mutex> lck(lock_);
      objVec_.push_back(obj);
      // cond_.notify_one();
    }
  }

  ObjectInfo *getRandomObject() {
    const int randomVal = rand() % objVec_.size();
    // VLOG(2) << "getting random=" << randomVal;
    return objVec_[randomVal];
  }
};

struct ThreadCtx {
  std::string device;
  int count{0};
  int objectSz{0};
  gobjfs::stats::StatsCounter<uint64_t> latency;
};

/*
 * Write thread context
 */
struct WrThreadCtx : public ThreadCtx {
  ObjectInfo *objectInfo{nullptr};
  Object *objectCache{nullptr}; /* Array of WROBJECT_CACHE_SZ */
  int objectCacheSz{0};
};

/*
 * Read thread context
 */
struct RdThreadCtx : public ThreadCtx {
  int alignSz{0};
};

// RdStatus
struct RdStatusExt {
  gRdStatus rdStatus;
  gRdObject *rdObject{nullptr};
  char *expectedBuf{nullptr};
  gobjfs::stats::Timer timer;
  uint64_t totalTimeMicrosec{0};
};

#define MAX_READERS 16

ObjectList objectList;
WrThreadCtx wrThreadCtx;
RdThreadCtx rdThreadCtx[MAX_READERS];
Config config;

std::atomic<uint64_t> totalIOPS{0};
gobjfs::stats::StatsCounter<uint64_t> totalLatency;

/*
 * Wait for read/write I/Os to complete
 */
static int wait_for_iocompletion(int epollfd, int efd, ThreadCtx *ctx,
                                 bool writerFlag) {
  gobjfs::stats::StatsCounter<uint32_t> readSizePerPoll;

  epoll_event *events =
      (struct epoll_event *)calloc(ctx->count, sizeof(epoll_event));

  int ctr = 0;
  while (ctr < ctx->count) {
    // LEVELTRIGGERED is default
    // Edge triggerd
    int n = epoll_wait(epollfd, events, ctx->count, -1);

    for (int i = 0; i < n; i++) {
      int ret = 0;
      if (efd != events[i].data.fd) {
        continue;
      }

      gIOStatus iostatus;
      ret = read(efd, &iostatus, sizeof(iostatus));

      if (ret != sizeof(iostatus)) {
        LOG(ERROR) << "Invalid IO Status of size: " << ret;
        continue;
      }

      VLOG(2) << "Received event"
              << " (completionId: " << iostatus.completionId
              << " status: " << iostatus.errorCode;

      ctr++;
      readSizePerPoll = ret;

      if (writerFlag) {
        ObjectInfo *obj = (ObjectInfo *)iostatus.completionId;
        assert(obj != nullptr);
        VLOG(2) << ctr << ") "
                << "ObjectID: " << obj->meta.objectId << " Written to:"
                << " Container: " << obj->meta.containerId
                << " object offset: " << obj->meta.objOffset
                << " Segment: " << obj->meta.segmentId;
        objectList.add(obj);
      } else {
        RdStatusExt *statusExt = (RdStatusExt *)iostatus.completionId;
        assert(statusExt != nullptr);
        gRdStatus *status = &statusExt->rdStatus;
        gRdObject *rdObject = statusExt->rdObject;
        assert(status->errorStatus == 0);

        statusExt->totalTimeMicrosec = statusExt->timer.elapsedMicroseconds();
        ctx->latency = statusExt->totalTimeMicrosec;

#ifdef MEMCHECK
        if (memcmp(rdObject->buf, statusExt->expectedBuf, rdObject->len) != 0) {
          LOG(ERROR) << "failed for "
                     << " Container: " << rdObject->containerId
                     << " offset: " << GETDISKOFFSET1(rdObject->containerId,
                                                      rdObject->objOffset,
                                                      rdObject->subObjOffset)
                     << " expected: " << std::string(statusExt->expectedBuf, 10)
                     << " actual: " << std::string(rdObject->buf, 10);
        }
#endif
        VLOG(2) << ctr << ")" << status->completionId
                << " Read completed Successfully";
      }
    }
    struct epoll_event event;
    event.data.fd = efd;
    event.events = EPOLLIN | EPOLLONESHOT;
    int s = epoll_ctl(epollfd, EPOLL_CTL_MOD, efd, &event);
    (void)s;
    assert(s == 0);
  }

  LOG(INFO) << "data per poll=" << readSizePerPoll;

  VLOG(2) << " wait_for_iocompletion writerFlag: " << writerFlag << " Exit !!";
  free(events);
  return 0;
}

static void reader_thread(void *arg) {
  assert(arg != nullptr);
  RdThreadCtx *ctx = (RdThreadCtx *)arg;
  LOG(INFO) << "Reader thread started "
            << "TotalsIOs to Perform: " << ctx->count;

  int epollfd = epoll_create1(0);
  int ret = 0;
  assert(epollfd >= 0);

  ObjectFSHandle_t handle;

  if (ObjectFS_Open(ctx->device.c_str(), O_RDONLY, &handle) < 0) {
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
  event.events = EPOLLIN | EPOLLONESHOT;
  int s = epoll_ctl(epollfd, EPOLL_CTL_ADD, efd, &event);
  assert(s == 0);
  (void)s;

  assert(ctx->count > 0);

  Object *objectCache = (Object *)malloc(sizeof(Object) * ctx->count);
  for (int i = 0; i < ctx->count; i++) {
    ret = posix_memalign((void **)&objectCache[i].data, ctx->alignSz,
                         ctx->objectSz);
    assert(ret == 0);
  }
  gRdObject *rdObject = (gRdObject *)malloc(sizeof(gRdObject) * ctx->count);
  RdStatusExt *rdStatusExt = new RdStatusExt[ctx->count];

  assert(objectCache != nullptr);
  assert(rdObject != nullptr);
  assert(rdStatusExt != nullptr);

  std::thread ioCompletionThread(wait_for_iocompletion, epollfd, efd, ctx,
                                 false);

  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < ctx->count; i++) {
    ObjectInfo *obj = objectList.getRandomObject(); /* Blocking call */
    assert(obj != nullptr);

    rdObject[i].buf = objectCache[i].data;
#if READ_4K
    rdObject[i].len = FOUR_K;
    // pick a random 4k offset boundary in the object
    rdObject[i].subObjOffset = FOUR_K * (rand() % (ctx->objectSz / FOUR_K));
#else
    rdObject[i].len = ctx->objectSz;
    rdObject[i].subObjOffset = 0;
#endif
    rdObject[i].containerId = obj->meta.containerId;
    // read the offset at which we wrote the object
    rdObject[i].objOffset = obj->meta.objOffset; // object starts here
    // rdStatusExt is sent as the completion ID and same is returned on I/O
    // completion. This contains rdObject which contains the data read from the
    // object and also the original data (obj->data) corresponding to the
    // metadata we're
    // reading (obj->meta)
    rdStatusExt[i].rdObject = &(rdObject[i]);
    rdStatusExt[i].expectedBuf = obj->data + rdObject[i].subObjOffset;

    rdObject[i].completionId = (gCompletionID) & (rdStatusExt[i]);

    VLOG(2) << "reading container=" << rdObject[i].containerId
            << "objoffset=" << rdObject[i].objOffset
            << "completionId=" << (void *)rdObject[i].completionId;

    // get start time for this object
    rdStatusExt[i].timer.reset();

    do {
      ret = ObjectFS_Get(handle, &rdObject[i], &(rdStatusExt[i].rdStatus), 1);

      if (ret) {
        std::cout << "Get error " << ret << std::endl;
      }
      VLOG(2) << "Read Buffer[idx=" << i << " ] sent. status:  " << ret;
    } while (ret == -EAGAIN);
    assert(ret == 0);
  }

  ioCompletionThread.join();
  auto end = std::chrono::high_resolution_clock::now();
  auto timeTaken = end - start;
  auto microsecs =
      std::chrono::duration_cast<std::chrono::microseconds>(timeTaken).count();
  uint64_t iops = ((uint64_t)ctx->count * 1000000) / microsecs;
  LOG(INFO) << "## READER THREAD"
            << " IOPS: " << iops << " IOSize: " << ctx->objectSz
            << " Total TimeTaken (in Sec): " << microsecs
            << " Read Latency: " << ctx->latency;

  totalIOPS += iops;

  free(objectCache);
  free(rdObject);
  delete[] rdStatusExt;

  ObjectFS_Close(&handle);
  VLOG(0) << "Reader Thread Exit";
}

/*
 * Writes #NUMOBJECTS objects in batches of #WRBATCHSZ
 */
void writer_thread(void *arg) {
  WrThreadCtx *ctx = (WrThreadCtx *)arg;

  int epollfd = epoll_create1(0);
  int ret = 0;
  assert(epollfd >= 0);

  LOG(INFO) << "Writer Thread Started. "
            << "TotalsIOs to Perform: " << ctx->count;

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
  event.events = EPOLLIN | EPOLLONESHOT;
  int s = epoll_ctl(epollfd, EPOLL_CTL_ADD, efd, &event);
  assert(s == 0);
  (void)s;

  assert(ctx->count > 0 && ctx->objectCacheSz > 0);

  Object *objectCache = ctx->objectCache;
  gWrObject *wrObject = (gWrObject *)malloc(sizeof(gWrObject) * ctx->count);
  ObjectInfo *objectInfo = ctx->objectInfo;

  assert(objectCache != nullptr);
  assert(objectInfo != nullptr);
  assert(wrObject != nullptr);

  std::thread ioCompletionThread(wait_for_iocompletion, epollfd, efd, ctx,
                                 true);

  /*
   * objectId - Unique ID generated from 0 to ctx->count
   * completionId - Points to objectInfo - size of number of IOs
   * buf - pointing to data in objectCache
   */

  auto start = std::chrono::high_resolution_clock::now();
  int objectCacheIdx = 0;
  for (int i = 0; i < ctx->count; i++) {

    objectInfo[i].data = objectCache[objectCacheIdx].data;

    wrObject[i].objectId = i;
    // objectInfo contains the data pointer for comparison with the same data
    // that
    // is sent for write. completion id is returned after each write.
    wrObject[i].completionId = (gCompletionID) & (objectInfo[i]);
    wrObject[i].buf = objectCache[objectCacheIdx].data;
    wrObject[i].len = ctx->objectSz;

    do {
      ret = ObjectFS_Put(handle, &wrObject[i], &(objectInfo[i].meta));

    } while (ret == -EAGAIN);
    assert(ret == 0);

    objectCacheIdx = (objectCacheIdx + 1) % ctx->objectCacheSz;
  }

  ioCompletionThread.join();
  auto end = std::chrono::high_resolution_clock::now();
  auto timeTaken = end - start;
  auto microsecs =
      std::chrono::duration_cast<std::chrono::microseconds>(timeTaken).count();
  uint64_t iops = ((uint64_t)ctx->count * 1000000) / microsecs;
  LOG(INFO) << "## WRITER THREAD"
            << " IOPS: " << iops << " IOSize: " << ctx->objectSz
            << " Total TimeTaken (in Sec): " << microsecs;

  totalIOPS += iops;
  free(wrObject);
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
  // log files are in /tmp
  google::InitGoogleLogging(argv[0]);

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
  WrThreadCtx wrThreadCtx;

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
    (void)ret;
    memset(objectCache[i].data, 'a' + (i % 26), config.objectSz);
  }

  int32_t writeIOps;
  /*
  if (config.fillData > 0) {
    writeIOps = config.fillData;
  } else {
   */
  writeIOps = config.totalWrIOps;
  //}

  ObjectInfo *objectInfo = (ObjectInfo *)malloc(sizeof(ObjectInfo) * writeIOps);
  assert(objectInfo != nullptr);

  wrThreadCtx.objectInfo = objectInfo;
  wrThreadCtx.device = config.device;
  wrThreadCtx.objectCache = objectCache;
  wrThreadCtx.objectCacheSz = config.objectCacheSz;
  wrThreadCtx.objectSz = config.objectSz;
  wrThreadCtx.count = writeIOps; // Number of Objects Created

  gobjfs::os::CpuStats startCpuStats;
  startCpuStats.getProcessStats();

  struct timeval start;
  { gettimeofday(&start, 0); }

  // Fill data set to +ve value if Read only operations to be perfomred
  // Data to be generated before read operations
  // if (config.fillData > 0) {
  {
    std::thread writerThread(writer_thread, (void *)&wrThreadCtx);
    writerThread.join();
    VLOG(1) << "Created " << config.fillData << " Objects in Objectlist";
    //} else {
    // if (config.maxWriterThreads > 0 && config.totalWrIOps > 0) {
    // threads.push_back(std::thread(writer_thread, (void*)&wrThreadCtx));
    //}
  }
  uint64_t elapsed = 0;
  {
    struct timeval end;
    gettimeofday(&end, 0);
    elapsed =
        (end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec;
  }
  gobjfs::os::CpuStats endCpuStats;
  endCpuStats.getProcessStats();

  endCpuStats -= startCpuStats;
  std::ostringstream s;
  s << "objectSz,numIOExec,IOPS,CpuPerc" << std::endl;
  s << config.objectSz << " , " << ObjectFS_getNumOfIOExecutors() << " , "
    << totalIOPS << " , "
    << ((endCpuStats.userTimeMicrosec_ + endCpuStats.systemTimeMicrosec_) *
        100) /
           elapsed << std::endl;

  totalIOPS = 0;

  google::FlushLogFiles(0);

  if (config.maxReaderThreads > 0 && config.totalRdIOps > 0) {

    startCpuStats.getProcessStats();
    gettimeofday(&start, 0);

    // rdThreadCtx = (RdThreadCtx*)malloc(sizeof (RdThreadCtx) *
    // config.maxReaderThreads);
    for (int i = 0; i < config.maxReaderThreads; i++) {
      VLOG(1) << "Creating Reader Thread " << i;
      int countPerThread =
          ceil((double)config.totalRdIOps / (double)config.maxReaderThreads);
      rdThreadCtx[i].count = countPerThread;
      rdThreadCtx[i].alignSz = config.alignSz;
      rdThreadCtx[i].objectSz = config.objectSz;
      rdThreadCtx[i].device = config.device;
      threads.push_back(std::thread(reader_thread, &(rdThreadCtx[i])));
    }

    for (auto it = threads.begin(); it != threads.end(); it++) {
      it->join();
    }

    {
      struct timeval end;
      gettimeofday(&end, 0);
      elapsed =
          (end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec;
    }

    endCpuStats.getProcessStats();
    endCpuStats -= startCpuStats;

    free(objectCache);

    for (int i = 0; i < config.maxReaderThreads; i++) {
      totalLatency += rdThreadCtx[i].latency;
    }

    s << "objectSz,numIOExec,readThreads,IOPS,latency,CpuPerc" << std::endl;
    s << config.objectSz << "," << ObjectFS_getNumOfIOExecutors() << ","
      << config.maxReaderThreads << "," << totalIOPS << ","
      << totalLatency.mean() << ","
      << ((endCpuStats.userTimeMicrosec_ + endCpuStats.systemTimeMicrosec_) *
          100) /
             elapsed;
  }

  ObjectFS_Destroy();

  LOG(INFO) << s.str();
  std::cout << s.str() << std::endl;
  return 0;
}
