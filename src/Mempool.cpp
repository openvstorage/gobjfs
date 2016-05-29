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

#include "Mempool.h"

#include <boost/lockfree/queue.hpp>
#include <cassert> // numeric_limits
#include <limits>  // numeric_limits
#include <sstream> // ostringstream

namespace gobjfs {

// ==================================

class AlignedMempool : public Mempool {
private:
  const size_t alignSize_ = 4096;

  struct Stats {
    std::atomic<uint64_t> bytesAllocated_{0};
    std::atomic<uint64_t> numAllocCalls_{0};
    std::atomic<uint64_t> numFailedAllocCalls_{0};
    std::atomic<uint64_t> numFreeCalls_{0};
  } stats_;

public:
  AlignedMempool(size_t alignSize = 4096);

  virtual void *Alloc(size_t size) override;

  virtual void Free(void *ptr) override;

  virtual size_t allocSize() override { return alignSize_; }

  virtual std::string GetStats() const override;
};

AlignedMempool::AlignedMempool(size_t alignSize) : alignSize_(alignSize) {}

void *AlignedMempool::Alloc(size_t size) {
  void *buffer = nullptr;
  int retcode = posix_memalign((void **)&buffer, alignSize_, size);
  if (retcode != 0) {
    stats_.numFailedAllocCalls_++;
    // TODO where to log retcode for error analysis ?
  } else {
    stats_.numAllocCalls_++;
    stats_.bytesAllocated_ += size;
  }
  return buffer;
}

void AlignedMempool::Free(void *ptr) {
  stats_.numFreeCalls_++;
  free(ptr);
}

std::string AlignedMempool::GetStats() const {
  std::ostringstream os;
  os << "for mempool thisptr=" << (void *)this
     << ":bytes alloc=" << stats_.bytesAllocated_
     << ":num alloc=" << stats_.numAllocCalls_
     << ":num failed=" << stats_.numFailedAllocCalls_
     << ":num free=" << stats_.numFreeCalls_ << std::endl;
  return os.str();
}

// =======================

class ObjectMempool : public Mempool {
  boost::lockfree::queue<void *> freeList{200};
  size_t objSize_{0};

public:
  ObjectMempool(size_t size) : objSize_(size) {}

  virtual void *Alloc(size_t allocSize) override {
    assert(allocSize == objSize_);
    void *ptr = nullptr;
    bool yes = false;
    if (!freeList.empty()) {
      yes = freeList.pop(ptr);
    }
    if (!yes) {
      return malloc(objSize_);
    }
    return ptr;
  }

  virtual void Free(void *ptr) override { freeList.push(ptr); }

  virtual std::string GetStats() const override {
    std::string str;
    return str;
  }

  virtual size_t allocSize() override { return objSize_; }
};

// =================================
// add std::forward args
MempoolSPtr MempoolFactory::createAlignedMempool(const std::string &name,
                                                 const size_t size) {
  return std::make_shared<AlignedMempool>(size);
}

MempoolSPtr MempoolFactory::createObjectMempool(const std::string &name,
                                                const size_t size) {
  return std::make_shared<ObjectMempool>(size);
}
}
