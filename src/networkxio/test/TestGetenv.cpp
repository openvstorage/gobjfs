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

#include <networkxio/gobjfs_getenv.h>
#include <gtest/gtest.h>

using namespace gobjfs::xio;

// Check if getenv_with_default works
TEST(NetworkXioTest, getenv_setenv) {

  int polltime = 3;
  int overwrite = 1;
  int err = setenv("GOBJFS_POLLING_TIME_USEC", std::to_string(polltime).c_str(), overwrite);
  ASSERT_EQ(err, 0);

  int value = getenv_with_default("GOBJFS_POLLING_TIME_USEC", 0);
  EXPECT_EQ(value, polltime);
}

TEST(NetworkXioTest, getenv_with_default) {

  int err = unsetenv("GOBJFS_POLLING_TIME_USEC");
  ASSERT_EQ(err, 0);

  int polltime = 2;
  int value = getenv_with_default("GOBJFS_POLLING_TIME_USEC", polltime);
  EXPECT_EQ(value, polltime);

}

