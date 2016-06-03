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

#include <cassert>
#include <cstdint>
#include <fcntl.h>
#include <pthread.h>
#include <sched.h> // getcpu
#include <string>

// To define gettid()
#include <sys/types.h>
#include <syscall.h>
#include <unistd.h>
#define gettid() (syscall(SYS_gettid))
// end define gettid()
//

typedef int16_t CoreId; // TODO make a full class later ?
#define CoreIdInvalid (-1)


namespace gobjfs {
namespace os {

static constexpr int32_t DirectIOSize = 512; // for posix_memalign

// https://blogs.oracle.com/jwadams/entry/macros_and_powers_of_two
inline bool IsDirectIOAligned(uint64_t number) {
  // check for 0 is special
  return (!number) || (!(number & (DirectIOSize - 1)));
}

inline size_t RoundToNext512(size_t numToRound) {
  constexpr uint32_t multiple = DirectIOSize;
  return (numToRound + multiple - 1) & ~(multiple - 1);
}

static constexpr int32_t FD_INVALID = -1;

inline bool IsFdOpen(int fd) { return (fcntl(fd, F_GETFL) != -1); }

void BindThreadToCore(CoreId cpu_id);

int32_t RaiseThreadPriority();

inline CoreId GetCpuCore() {
  thread_local CoreId thisCore_ = sched_getcpu();
  return (thisCore_ >= 0) ? thisCore_ : 0;
}
}
} // namespace
