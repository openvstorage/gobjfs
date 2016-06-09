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

#include "CpuStats.h"

#include <cassert>
#include <glog/logging.h>
#include <sstream>
#include <string.h>       // memcpy
#include <sys/resource.h> // rusage
#include <sys/time.h>     // rusage

#define SEC_TO_MICROSEC 1000000

namespace gobjfs {
namespace os {

int32_t CpuStats::getFromKernel(bool isProcess) {
  struct rusage usageInfo;
  int who = (isProcess ? RUSAGE_SELF : RUSAGE_THREAD);
  int err = getrusage(who, &usageInfo);
  if (err != 0) {
    err = -errno;
    LOG(ERROR) << "getrusage failed errno=" << err;
    return err;
  }

  userTimeMicrosec_ =
      usageInfo.ru_utime.tv_sec * SEC_TO_MICROSEC + usageInfo.ru_utime.tv_usec;
  systemTimeMicrosec_ =
      usageInfo.ru_stime.tv_sec * SEC_TO_MICROSEC + usageInfo.ru_stime.tv_usec;

  timeval wallTime;
  int ret = gettimeofday(&wallTime, 0);
  if (ret != 0) {
    LOG(WARNING) << "gettimeofday failed with errno=" << errno;
  } else {
    wallTimeMicrosec_ = (wallTime.tv_sec * SEC_TO_MICROSEC) + wallTime.tv_usec;
  }

  voluntaryCtxSwitch_ = usageInfo.ru_nvcsw;
  involuntaryCtxSwitch_ = usageInfo.ru_nivcsw;

  return err;
}

int32_t CpuStats::getProcessStats() { return getFromKernel(true); }

int32_t CpuStats::getThreadStats() { return getFromKernel(false); }

int32_t CpuStats::getCpuUtilization() { return cpuUtil_; }

CpuStats::CpuStats(const CpuStats &other) {
  memcpy(this, &other, sizeof(*this));
}

void CpuStats::operator=(const CpuStats &other) {
  memcpy(this, &other, sizeof(*this));
}

CpuStats &CpuStats::operator-=(const CpuStats &other) {
  userTimeMicrosec_ -= other.userTimeMicrosec_;
  systemTimeMicrosec_ -= other.systemTimeMicrosec_;
  voluntaryCtxSwitch_ -= other.voluntaryCtxSwitch_;
  involuntaryCtxSwitch_ -= other.involuntaryCtxSwitch_;

  wallTimeMicrosec_ -= other.wallTimeMicrosec_;

  float totalCpuTime = userTimeMicrosec_ + systemTimeMicrosec_;
  // calc percentage cpu utilization
  cpuUtil_ = (totalCpuTime * 100) / wallTimeMicrosec_;

  return *this;
}

CpuStats CpuStats::operator-(const CpuStats &other) {
  CpuStats diff(*this);
  diff -= other;
  return diff;
}

std::string CpuStats::ToString() const {
  std::ostringstream s;

  s << "{\"user_time(us)\":" << userTimeMicrosec_
    << ",\"system_time(us)\":" << systemTimeMicrosec_
    << ",\"wall_time(us)\":" << wallTimeMicrosec_
    << ",\"vol ctx switch\":" << voluntaryCtxSwitch_
    << ",\"invol ctx switch\":" << involuntaryCtxSwitch_
    << "}";

  return s.str();
}
}
}
