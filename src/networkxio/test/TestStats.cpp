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

#include <util/os_utils.h>
#include <gobjfs_client.h>

#include <gobjfs_log.h>
#include <gtest/gtest.h>

using namespace gobjfs::xio;

int portNumber = 21321;

// Check for errors if ctx or NetworkXioClient is null
TEST(NetworkXioClientTest, StatsWithoutCtx) {

  auto ctx_attr = ctx_attr_new();

  ctx_attr_set_transport(ctx_attr,
                                       "tcp",
                                       "127.0.0.1",
                                       portNumber);

  client_ctx_ptr ctx;

  auto str = ctx_get_stats(ctx);

  EXPECT_EQ(str, "invalid ctx or client connection ctx=0");
}

TEST(NetworkXioClientTest, StatsWithoutClient) {

  auto ctx_attr = ctx_attr_new();

  ctx_attr_set_transport(ctx_attr,
                                       "tcp",
                                       "127.0.0.1",
                                       portNumber);

  auto ctx = ctx_new(ctx_attr);

  auto str = ctx_get_stats(ctx);

  EXPECT_NE(str.find("netclient=0"), std::string::npos);
  EXPECT_NE(str.find("invalid ctx or client connection"), std::string::npos);
}

