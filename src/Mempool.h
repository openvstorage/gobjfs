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

#pragma once

#include <atomic>
#include <memory> // shared_ptr
#include <string>

namespace gobjfs {

class Mempool {
protected:

  struct Stats {
    std::atomic<uint64_t> bytesAllocated_{0};
    std::atomic<uint64_t> numAllocCalls_{0};
    std::atomic<uint64_t> numFailedAllocCalls_{0};
    std::atomic<uint64_t> numFreeCalls_{0};
    std::atomic<uint64_t> numReused_{0};
  } stats_;

public:
  virtual void *Alloc(size_t size) = 0;

  virtual void Free(void *ptr) = 0;

  virtual std::string GetStats() const = 0;

  virtual size_t allocSize() = 0;

  virtual ~Mempool() {}
};

typedef std::shared_ptr<Mempool> MempoolSPtr;

class MempoolFactory {
public:
  static MempoolSPtr createAlignedMempool(const std::string &name,
                                          const size_t alignSize);

  static MempoolSPtr createObjectMempool(const std::string &name,
                                         const size_t objSize);
};
}
