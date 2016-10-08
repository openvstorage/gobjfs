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

#include <chrono>

namespace gobjfs {
namespace stats {

/**
 * Usage { Timer t1; {...}; t1.elapsedSeconds(); }
 */
class Timer {
private:
  typedef std::chrono::high_resolution_clock Clock;
  Clock::time_point begin_;

public:
  explicit Timer(bool startNow = false) {
    if (startNow)
      reset();
  }

  void reset() { begin_ = Clock::now(); }

  int64_t differenceMicroseconds(const Timer& older) const {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               begin_ - older.begin_).count();
  }

  int64_t differenceMilliseconds(const Timer& older) const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               begin_ - older.begin_).count();
  }

  int64_t differenceNanoseconds(const Timer& older) const {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               begin_ - older.begin_).count();
  }

  int64_t elapsedSeconds() const {
    return std::chrono::duration_cast<std::chrono::seconds>(Clock::now() -
                                                            begin_).count();
  }

  int64_t elapsedMilliseconds() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               Clock::now() - begin_).count();
  }

  int64_t elapsedMicroseconds() const {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               Clock::now() - begin_).count();
  }

  int64_t elapsedNanoseconds() const {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() -
                                                                begin_).count();
  }
};
}
} // namespace
