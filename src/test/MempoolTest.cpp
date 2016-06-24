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

#include "../Mempool.h"
#include <gMempool.h>
#include <gobjfs_log.h>
#include <gtest/gtest.h>
#include <util/os_utils.h>
#include <string.h> // strstr

using gobjfs::MempoolSPtr;
using gobjfs::MempoolFactory;

using gobjfs::os::IsDirectIOAligned;
using gobjfs::os::DirectIOSize;

TEST(MempoolTest, CheckAlignment) {
  MempoolSPtr m = MempoolFactory::createAlignedMempool("aligned", DirectIOSize);

  void *p = m->Alloc(4097);

  EXPECT_TRUE(IsDirectIOAligned((uint64_t)p));
}

TEST(MempoolTest, CheckStats) {
  MempoolSPtr m = MempoolFactory::createAlignedMempool("aligned", DirectIOSize);

  void *p = m->Alloc(2048);

  {
    auto s = m->GetStats();
    const std::string statsString =
        "bytes alloc=2048:num alloc=1:num failed=0:num free=0";
    auto loc = s.find(statsString);
    EXPECT_TRUE(loc > 0);
  }

  m->Free(p);
  {
    auto s = m->GetStats();
    const std::string statsString =
        "bytes alloc=2048:num alloc=1:num failed=0:num free=1";
    auto loc = s.find(statsString);
    EXPECT_TRUE(loc > 0);
  }
}

TEST(gMempoolTest, CheckAlignment) {
  gMempool_init(DirectIOSize);

  void *p = gMempool_alloc(4097);

  EXPECT_TRUE(IsDirectIOAligned((uint64_t)p));
}

TEST(gMempoolTest, CheckStats) {
  gMempool_init(DirectIOSize);

  void *p = gMempool_alloc(2048);

  {
    char buf[8192];
    gMempool_getStats(buf, 8192);
    const std::string statsString =
        "bytes alloc=2048:num alloc=1:num failed=0:num free=0";
    auto loc = strstr(buf, statsString.c_str());
    EXPECT_TRUE(loc != nullptr);
  }

  gMempool_free(p);
  {
    char buf[8192];
    gMempool_getStats(buf, 8192);
    const std::string statsString =
        "bytes alloc=2048:num alloc=1:num failed=0:num free=1";
    auto loc = strstr(buf, statsString.c_str());
    EXPECT_TRUE(loc != nullptr);
  }
}
