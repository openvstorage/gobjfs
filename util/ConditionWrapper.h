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

#include <condition_variable>
#include <mutex>

namespace gobjfs {
namespace os {

class ConditionWrapper {
public:
  std::mutex mutex_;
  std::condition_variable cond_;

  void pause() {
    std::unique_lock<std::mutex> lck(mutex_);
    cond_.wait(lck);
  }

  void pauseIf(std::function<bool(void)> pred) {
    std::unique_lock<std::mutex> lck(mutex_);
    cond_.wait(lck, pred);
  }

  void wakeup() {
    std::unique_lock<std::mutex> lck(mutex_);
    cond_.notify_all();
  }

  void wakeupIf(std::function<bool(void)> pred) {
    std::unique_lock<std::mutex> lck(mutex_);
    if (pred() == true)
      cond_.notify_all();
  }
};
}
}
