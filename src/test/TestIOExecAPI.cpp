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

#include <gIOExecFile.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <util/os_utils.h>
#include <fcntl.h>
#include <sys/types.h>

/*
 * for a null service handle,
 * eventFdOpen, FileOpen, FileDelete, ServiceDestroy
 * must fail
 */

TEST(IOExecFile, NoInitDone) {
  IOExecServiceHandle serviceHandle = nullptr;

  auto evHandle = IOExecEventFdOpen(serviceHandle);
  EXPECT_EQ(evHandle, nullptr);

  auto fd = IOExecEventFdGetReadFd(evHandle);
  EXPECT_EQ(fd, gobjfs::os::FD_INVALID);

  auto handle = IOExecFileOpen(serviceHandle, "/tmp/abc", O_RDWR | O_CREAT);
  EXPECT_EQ(handle, nullptr);

  auto ret = IOExecFileDelete(serviceHandle, "/tmp/abc", 0, evHandle);
  EXPECT_NE(ret, 0);

  ret = IOExecFileServiceDestroy(serviceHandle);
  EXPECT_NE(ret, 0);
}
