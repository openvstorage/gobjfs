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
#include <vector>

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

namespace boost {
namespace program_options {
class options_description;
}
}

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
 *global
 *epollFD_
 */
class FilerCtx {
public:
  io_context_t ioCtx_;

  int32_t ioQueueDepth_;
  std::atomic<int32_t> numAvailable_{0};

public:
  explicit FilerCtx();

  int32_t init(int32_t queueDepth);

  ~FilerCtx();

  std::string getState() const;

  bool isEmpty() const { return (numAvailable_ == 0); }

  bool isFull() const { return (numAvailable_ == ioQueueDepth_); }

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

    uint32_t maxFdQueueSize_;

    void setDerivedParam();

    explicit Config(); // use defaults

    explicit Config(uint32_t queueDepth);

    void print() const;

    // add options needed by IOExecutor to parser config
    int addOptions(boost::program_options::options_description &desc);
  };

  static Config defaultConfig_;
  Config config_;

  struct Statistics {
    // Most variables are incremented by a single thread
    // but it is *not* the same thread that updates all

    std::atomic<uint64_t> numQueued_{0};    // multi-thread writers
    std::atomic<uint64_t> numSubmitted_{0}; // multi-thread writers
    std::atomic<uint64_t> numCompleted_{0}; // multi-thread writers

    // updated by completionThread
    struct OpStats {
      gobjfs::stats::StatsCounter<int64_t> waitTime_;
      gobjfs::stats::StatsCounter<int64_t> serviceTime_;

      gobjfs::stats::Histogram<int64_t> waitHist_;
      gobjfs::stats::Histogram<int64_t> serviceHist_;

      uint32_t numOps_ = 0;
      uint32_t numBytes_ = 0;

      std::string getState() const;
    };

    // maintain per-op statistics
    OpStats write_;
    OpStats nonAlignedWrite_;
    OpStats read_;
    OpStats delete_;

    gobjfs::stats::MaxValue<uint32_t> maxRequestQueueSize_;
    gobjfs::stats::MaxValue<uint32_t> maxFdQueueSize_;

    gobjfs::stats::StatsCounter<int64_t> numProcessedInLoop_;

    uint32_t idleLoop_ = 0;
    uint32_t numCompletionEvents_ = 0;

    uint32_t requestQueueLow1_ = 0;
    uint32_t requestQueueLow2_ = 0;
    uint32_t requestQueueFull_ = 0;

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

  int handleXioEvent(int fd, int events, void* data);

  virtual void stop();

  std::string getState() const;

private:

  virtual void execute();

  int32_t ProcessRequestQueue();
  int32_t ProcessFdQueue();

  void ProcessCompletions();
  int32_t ProcessCallbacks(io_event *events, int32_t n_events);
  int32_t doPostProcessingOfJob(FilerJob *job);

  uint32_t minSubmitSize_{1};

  // Requests added by submitTask
  boost::lockfree::queue<FilerJob *> requestQueue_;
  ConditionWrapper requestQueueHasSpace_;
  std::atomic<int32_t> requestQueueSize_{0};

  FilerCtx ctx_;
};

typedef std::shared_ptr<IOExecutor> IOExecutorSPtr;

} // namespace
