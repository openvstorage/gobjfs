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

#include <cstdint>

namespace gobjfs {
namespace os {

/**
 * wrapper around eventfd which is used to send shutdown
 * message to any thread waiting in epoll_wait()
 */
class ShutdownNotifier {
  int fd_ = -1;

public:
  int32_t init();

  int getFD() const { return fd_; }

  int32_t send();

  int32_t recv(uint64_t &counter);

  int32_t destroy();

  ~ShutdownNotifier();
};
}
}
