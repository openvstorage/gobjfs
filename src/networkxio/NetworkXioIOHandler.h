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

#include "NetworkXioRequest.h"
#include <gcommon.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <assert.h>
#include <list>

#include "gIOExecFile.h"
#include <util/TimerNotifier.h>
#include <util/Stats.h>

namespace gobjfs {
  class IOExecutor;
}

using gobjfs::stats::StatsCounter;
using gobjfs::os::TimerNotifier;

namespace gobjfs {
namespace xio {

static int static_runEventHandler(gIOStatus& iostatus, void* ctx);

class NetworkXioIOHandler {
public:
  NetworkXioIOHandler(PortalThreadData* pt);

  ~NetworkXioIOHandler();

  NetworkXioIOHandler(const NetworkXioIOHandler &) = delete;

  NetworkXioIOHandler &operator=(const NetworkXioIOHandler &) = delete;

  // @return whether req is finished 
  bool process_request(NetworkXioRequest *req);

  void handle_request(NetworkXioRequest *req);

  /**
   * handler called from accelio event loop
   * to print periodic stats
   */
  void runTimerHandler();

  size_t numPendingRequests();

  void drainQueue();

private:
  void handle_open(NetworkXioRequest *req);

  int handle_read(NetworkXioRequest *req, const std::string &filename,
                  size_t size, off_t offset);

  void handle_error(NetworkXioRequest *req, int errval);

  void startEventHandler();

  int runEventHandler(gIOStatus& iostatus);

  void stopEventHandler();
  

  /**
   * this func is passed as argument to IOExecFileRead
   * it gets called from FilerJob::reset
   */
  friend int static_runEventHandler(gIOStatus& iostatus, void* ctx);

private:
  std::string configFileName_;

  IOExecServiceHandle serviceHandle_{nullptr};

  IOExecEventFdHandle eventHandle_{nullptr};

  // fd on which disk IO completions are received
  // this is one per portal thread
  // it is passed to IOExecutor when a disk IO job is submitted
public:
  int eventFD_{-1};
private:

  StatsCounter<uint32_t> workQueueLen_;

  std::unique_ptr<TimerNotifier> statsTimerFD_;

  // pointer to parent 
  PortalThreadData* pt_;

  float prevBatchSize_{0.0f};
  uint64_t prevOps_{0};
  int incrDirection_{1}; // -1 or +1
  int timerCalled_{0}; 
  gobjfs::stats::StatsCounter<uint32_t> minSubmitSizeStats_;

public:
  IOExecutor* ioexecPtr_{nullptr};

  friend class PortalThreadData;

};

typedef std::unique_ptr<NetworkXioIOHandler> NetworkXioIOHandlerPtr;
}
} // namespace
