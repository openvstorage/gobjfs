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

#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <libaio.h>
#include <string>

#include <Executor.h>
#include <IOExecutor.h>

#include <util/ConditionWrapper.h>
#include <util/SemaphoreWrapper.h>
#include <util/ShutdownNotifier.h>
#include <util/TimerNotifier.h>

#include <util/CpuStats.h>
#include <util/Stats.h>
#include <util/Timer.h>
#include <util/os_utils.h>

namespace gobjfs {

class FilerJob;
class IOExecutor;

using gobjfs::os::ConditionWrapper;
using gobjfs::os::TimerNotifier;
using gobjfs::os::ShutdownNotifier;
using gobjfs::os::SemaphoreWrapper;
using gobjfs::os::FD_INVALID;
using gobjfs::os::CpuStats;

/*
 * FilerCtx:
 *	- FilerCtx is an async io context wrapper
 *	- Structure keeps track of parameters like
 *		ioQueueDepth_ : queue depth of io_context at async layer
 *		eventFD_			: eventfd to poll for async io
 *completions
 *		epollFD_			: Not used here. But set to
 *global
 *epollFD_
 */
class FilerCtx {
public:
  io_context_t ioCtx_;

  int epollFD_ = FD_INVALID;
  int eventFD_ = FD_INVALID;

  int32_t ioQueueDepth_;
  std::atomic<int32_t> numAvailable_{0};

public:
  explicit FilerCtx();

  int32_t init(int32_t queueDepth, int epollFD);

  ~FilerCtx();

  std::string getState() const;

  bool isEmpty() const { return (numAvailable_ == 0); }

  void incrementNumAvailable(int32_t count = 1) {
    numAvailable_ += count;
    assert(numAvailable_ <= ioQueueDepth_);
  }
  void decrementNumAvailable(int32_t count) {
    numAvailable_ -= count;
    assert(numAvailable_ >= 0);
  }

  GOBJFS_DISALLOW_COPY(FilerCtx);
  GOBJFS_DISALLOW_MOVE(FilerCtx);
};

class IOExecutor : public Executor {
public:
  struct Config {

  public:
    bool noSubmitterThread_{true};

    std::vector<CoreId> cpuCores_;

    uint32_t queueDepth_ = 200;
    // 200 is good default for NVME SSDs
    // if we run on other disks, lets abstract this out

    uint32_t minSubmitSize_ = 16;

    // maxRequestQueueSize need not be more than io contexts available
    uint32_t maxRequestQueueSize_;

    void setDerivedParam();

    explicit Config(); // use defaults

    explicit Config(uint32_t queueDepth);

    void print() const;
  };

  static Config defaultConfig_;
  Config config_;

  struct Statistics {
    // Most variables are incremented by a single thread
    // but it is *not* the same thread that updates all
    uint32_t numWrites_ = 0;
    uint32_t numReads_ = 0;
    uint64_t bytesWritten_ = 0;
    uint64_t bytesRead_ = 0;

    uint32_t numDeletes_ = 0;

    std::atomic<uint64_t> numQueued_{0};    // multi-thread writers
    std::atomic<uint64_t> numSubmitted_{0}; // multi-thread writers
    uint64_t numCompleted_{0};

    // updated by completionThread
    gobjfs::stats::StatsCounter<int64_t> waitTime_;
    gobjfs::stats::StatsCounter<int64_t> serviceTime_;

    gobjfs::stats::Histogram<int64_t> waitHist_;
    gobjfs::stats::Histogram<int64_t> serviceHist_;

    gobjfs::stats::MaxValue<uint32_t> maxRequestQueueSize_;
    gobjfs::stats::MaxValue<uint32_t> maxFinishQueueSize_;

    gobjfs::stats::StatsCounter<int64_t> numProcessedInLoop_;

    uint32_t idleLoop_ = 0;
    uint32_t numCompletionEvents_ = 0;

    uint32_t requestQueueLow1_ = 0;
    uint32_t requestQueueLow2_ = 0;
    uint32_t requestQueueFull_ = 0;

    CpuStats completionThread_;
    CpuStats submitterThread_;
    CpuStats fdQueueThread_;

    void incrementOps(FilerJob *job);

    void clear() { bzero(this, sizeof(*this)); }

    void print() const;

    std::string getState() const;
  } stats_;

  explicit IOExecutor(const std::string &instanceName,
                      CoreId core = CoreIdInvalid,
                      const Config &config = defaultConfig_);

  virtual ~IOExecutor();

  GOBJFS_DISALLOW_COPY(IOExecutor);
  GOBJFS_DISALLOW_MOVE(IOExecutor); // dont move executing obj

  int32_t submitTask(FilerJob *job, bool blockIfQueueFull);

  virtual void stop();

  std::string getState() const;

private:
  virtual void execute();

  int32_t ProcessRequestQueue();
  int32_t ProcessFdQueue();

  void ProcessCompletions();
  int32_t ProcessCallbacks(io_event *events, int32_t n_events);
  int32_t doPostProcessingOfJob(FilerJob *job);

  std::thread submitterThread_;
  ConditionWrapper submitterCond_;
  SemaphoreWrapper ctxCond_; // signals if ctx available

  // set when submitterThread waits for new requests
  std::atomic<bool> submitterWaitingForNewRequests_{false};

  // set when submitterThread waits for free io_context
  std::atomic<bool> submitterWaitingForFreeCtx_{false};

  uint32_t minSubmitSize_{1};

  std::thread completionThread_;
  ShutdownNotifier completionThreadShutdown_;
  // writing to this fd causes completionThread_ to exit
  //
  TimerNotifier periodicTimer_;

  // Requests added by submitTask
  boost::lockfree::queue<FilerJob *> requestQueue_;
  ConditionWrapper requestQueueHasSpace_;
  std::atomic<int32_t> requestQueueSize_{0};

  // for metadata ops (create, delete, sync)
  std::thread fdQueueThread_;
  SemaphoreWrapper fdQueueCond_; // signals if fdQueue has new elem
  boost::lockfree::queue<FilerJob *> fdQueue_;
  std::atomic<int32_t> fdQueueSize_{0};
  // size variables are kept as "signed" to catch
  // increment/decrement errors

  // fd on which completion thread waits
  int epollFD_ = FD_INVALID;

  FilerCtx ctx_;
};

typedef std::shared_ptr<IOExecutor> IOExecutorSPtr;

} // namespace
