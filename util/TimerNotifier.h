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
 * wrapper around timerfd which is used to wakeup
 * any thread waiting in epoll_wait()
 */
class TimerNotifier {
  int fd_ = -1;

public:
  int32_t init(int epollFD, int timeoutSec, int timeoutNanosec);

  int32_t send();

  int32_t recv();

  int32_t destroy();

  ~TimerNotifier();
};
}
}
