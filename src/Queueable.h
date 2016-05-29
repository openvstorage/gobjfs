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

#include <assert.h>
#include <memory>
#include <util/Timer.h>
#include <util/lang_utils.h>

namespace gobjfs {

/**
 * Any item which goes into a queue can have its
 * wait time and service time recorded, by deriving
 * from this class.  Statistics framework will
 * use this info
 */
class Queueable {
  int64_t waitTime_{0};
  // time diff between submitTask and dequeue

  int64_t serviceTime_{0};
  // time diff between dequeue and done

  gobjfs::stats::Timer timer_;

public:
  explicit Queueable() : waitTime_(0), serviceTime_(0) {}

  ~Queueable() { waitTime_ = serviceTime_ = 0; }

  GOBJFS_DISALLOW_COPY(Queueable);
  GOBJFS_DISALLOW_MOVE(Queueable);

  int64_t waitTime() const { return waitTime_; }
  int64_t serviceTime() const { return serviceTime_; }

  void setSubmitTime() {
    timer_.reset();
    assert(waitTime_ == 0);
    assert(serviceTime_ == 0);
  }

  void setWaitTime() {
    assert(waitTime_ == 0);
    assert(serviceTime_ == 0);
    waitTime_ = timer_.elapsedNanoseconds();
    timer_.reset();
  }

  void setServiceTime() {
    assert(waitTime_ != 0);
    assert(serviceTime_ == 0);
    serviceTime_ = timer_.elapsedNanoseconds();
  }
};
}
