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
#include <semaphore.h>

namespace gobjfs {
namespace os {

class SemaphoreWrapper {
public:
  sem_t semaphore_;

  SemaphoreWrapper() {}

  int32_t init(unsigned int initVal, int epollfd);

  int32_t pause();

  int32_t wakeup(uint64_t count = 1);

  int getValue();

  ~SemaphoreWrapper();
};
}
}
