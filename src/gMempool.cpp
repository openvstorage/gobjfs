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

#include <Mempool.h>
#include <gMempool.h>
#include <string.h>
#include <gobjfs_log.h>
#include <util/os_utils.h>

using gobjfs::Mempool;
using gobjfs::MempoolFactory;
using gobjfs::MempoolSPtr;
using gobjfs::os::RoundToNext512;

// TODO: add more pools if required
// Keep a std::map<pool name, MemPool>
// Or pass back a handle in gMempool_init
static MempoolSPtr pool;

int gMempool_init(size_t alignSize) {
  pool = MempoolFactory::createAlignedMempool("aligned", alignSize);
  if (pool.get())
    return 0;
  LOG(ERROR) << "failed to allocate Mempool";
  return -1;
}

void *gMempool_alloc(size_t size) {
  LOG_IF(FATAL, pool == nullptr) << "call to gMempool_init is missing";
  const size_t allocSize = RoundToNext512(size);
  return pool->Alloc(allocSize);
}

void gMempool_free(void *ptr) {
  LOG_IF(FATAL, pool == nullptr) << "call to gMempool_init is missing";
  pool->Free(ptr);
}

void gMempool_getStats(char *buffer, size_t len) {
  LOG_IF(FATAL, pool == nullptr) << "call to gMempool_init is missing";
  const auto str = pool->GetStats();
  strncpy(buffer, str.data(), len);
}
