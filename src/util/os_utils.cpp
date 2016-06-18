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

#include <glog/logging.h>
#include <util/os_utils.h>

#include <errno.h>        // errno
#include <sched.h>        // sched_getscheduler
#include <sys/resource.h> //getrlimit
#include <sys/time.h>     //getrlimit

namespace gobjfs {
namespace os {

void BindThreadToCore(CoreId cpu_id) {
  cpu_set_t cs;
  CPU_ZERO(&cs);
  CPU_SET(cpu_id, &cs);
  auto r = pthread_setaffinity_np(pthread_self(), sizeof(cs), &cs);
  assert(r == 0);
  LOG(INFO) << "bound thread=" << gettid() << " to core=" << cpu_id;
}

// to set realtime priorities
// add rtprio to /etc/security/limits.conf
// add pam_limits.so to /etc/pam.d/common-* files
// verify "ulimit -r" is nonzero
int32_t RaiseThreadPriority() {
  int ret = -EINVAL;

  do {
    rlimit rlim;
    ret = getrlimit(RLIMIT_RTPRIO, &rlim);
    if (ret == -1) {
      LOG(ERROR) << "Failed to get rtprio limit errno=" << -errno;
      break;
    }
    if ((rlim.rlim_cur == 0) || (rlim.rlim_max == 0)) {
      LOG(ERROR)
          << "rtprio not enabled in limits.conf or in pam.d/common files. "
          << "Current=" << rlim.rlim_cur << ":Max=" << rlim.rlim_max;
      break;
    }

    sched_param param;
    param.sched_priority = rlim.rlim_max;
    ret = sched_setscheduler(0, SCHED_RR, &param);
    if (ret != 0) {
      LOG(ERROR) << " Failed to raise priority.  errno=" << -errno;
      break;
    }

    ret = sched_getscheduler(0);
    if (ret == -1) {
      LOG(ERROR) << "Failed to fetch sched policy " << -errno;
    } else if (ret == 0) {
      LOG(ERROR) << "Failed to change sched policy. It is still=" << ret;
      ret = -EINVAL;
    } else if (ret == SCHED_RR) {
      ret = 0;
    }
  } while (0);
  return ret;
}
}
}
