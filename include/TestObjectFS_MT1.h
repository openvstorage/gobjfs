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
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <unistd.h>

#define TESTFILE "/dev/sdb"
#define IOCONFIGFILE "/etc/gobjfs/TestObjectFS.conf"

#define OBJECTSZ 16384 /* Length of object is fixed */
#define ALIGNSZ 4096

#define WRTHREADS 1
#define RDTHREADS 4

#define NUMOBJECTS 100000 /* Total Number of Objects written */
#define WRBATCHSZ 100
#define NUMREADS 100000 /* Reads per thread */
#define RDBATCHSZ 100

#define WROBJECT_CACHESZ 10000

#define READOP false
#define WRITEOP true

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
      std::unique_lock<std::mutex> lck(lock_);
      objVec_.push_back(obj);
      cond_.notify_one();
    }
  }

  ObjectInfo *getRandomObject() {
    ObjectInfo *obj = nullptr;
    {
      std::unique_lock<std::mutex> lck(lock_);
      cond_.wait(lck, [this] { return (!this->objVec_.empty()); });

      std::vector<ObjectInfo *>::iterator randIt = objVec_.begin();
      std::advance(randIt, std::rand() % objVec_.size());
      obj = *randIt;
    }
    return obj;
  }
};

struct RdStatusExt {
  gRdStatus rdStatus;
  gRdObject *rdObject;
  char *expectedBuf;
};

/*
 * Read thread context
 */
struct RdThreadCtx {
  int count;
  int batchSz;
};

/*
 * Write thread context
 */
struct WrThreadCtx {
  ObjectInfo *objectInfo; /* Array of WRBATCH size */
  Object *objectCache;    /* Array of WROBJECT_CACHE_SZ */
  int count;
  int batchSz;
  int objectCacheSz;
};

WrThreadCtx wrThreadCtx;
RdThreadCtx rdThreadCtx[RDTHREADS];

ObjectList ObjectList; /* Complete ObjectList written on disk */
