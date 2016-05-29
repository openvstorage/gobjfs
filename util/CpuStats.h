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
#include <string>

namespace gobjfs {
namespace os {

struct CpuStats {

  uint64_t userTimeMicrosec_{0};   // getrusage
  uint64_t systemTimeMicrosec_{0}; // getrusage
  uint64_t wallTimeMicrosec_{0};   // gettimeofday
  int32_t cpuUtil_{0};

  long voluntaryCtxSwitch_{0};
  long involuntaryCtxSwitch_{0};

public:
  CpuStats() = default;

  // subtract "end - start" cpustats
  CpuStats operator-(const CpuStats &other);
  CpuStats &operator-=(const CpuStats &other);

  int32_t getFromKernel(bool isProcess);

  int32_t getProcessStats();

  int32_t getThreadStats();

  // Returns number in range [1, N * 100]
  // where N = number of CPU
  int32_t getCpuUtilization();

  std::string ToString() const;

  CpuStats(const CpuStats &);

  void operator=(const CpuStats &);

  uint64_t userTimeMicrosec() const { return userTimeMicrosec_; }
  uint64_t systemTimeMicrosec() const { return systemTimeMicrosec_; }
  uint64_t userTimeSec() const { return userTimeMicrosec_ / 1000000; }
  uint64_t systemTimeSec() const { return systemTimeMicrosec_ / 1000000; }
};
}
}
