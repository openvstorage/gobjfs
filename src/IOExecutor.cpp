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

#include "IOExecutor.h"
#include "FilerJob.h"
#include <gcommon.h>

#include <errno.h>
#include <gobjfs_log.h>
#include <sstream>     // open
#include <sys/epoll.h> // epoll_event
#include <util/os_utils.h>

#include <sstream>       //
#include <sys/eventfd.h> // EFD_NONBLOCk
#include <boost/program_options.hpp>

using namespace gobjfs;
using namespace gobjfs::stats;
using gobjfs::os::IsDirectIOAligned;

#define EPOLL_MAXEVENT 10 // arbitrary number

namespace gobjfs {

FilerCtx::FilerCtx() {}

int32_t FilerCtx::init(int32_t queueDepth, int epollFD) {
  ioQueueDepth_ = queueDepth;
  epollFD_ = epollFD;

  int retcode = 0;

  do {
    bzero(&ioCtx_, sizeof(ioCtx_));
    retcode = io_queue_init(queueDepth, &ioCtx_);
    if (retcode != 0) {
      LOG(ERROR) << "Failed to init io queue errno=" << -errno;
      break;
    }
    numAvailable_ = ioQueueDepth_;

    retcode = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (retcode == -1) {
      LOG(ERROR) << "Failed to create eventfd errno=" << -errno;
      break;
    }
    eventFD_ = retcode;

    epoll_event epollEvent;
    bzero(&epollEvent, sizeof(epollEvent));

    // since event.data is set to contain pointer to ctx,
    // it will be returned by epoll_wait()
    // this way, you can find FilerCtx from eventFD
    epollEvent.data.ptr = (void *)this;

    epollEvent.events = EPOLLIN | EPOLLPRI | EPOLLERR | EPOLLHUP;
    retcode = epoll_ctl(epollFD_, EPOLL_CTL_ADD, eventFD_, &epollEvent);
    if (retcode != 0) {
      LOG(ERROR) << "Failed to add epoll errno=" << -errno;
      break;
    }

    LOG(INFO) << "epoll registered filer fd=" << eventFD_
              << " with ptr=" << (void *)this;

  } while (0);

  return retcode;
}

FilerCtx::~FilerCtx() {
  int retcode = 0;
  assert(epollFD_ >= 0);

  /* IOExecutor itself is being destroyed
  int retcode = epoll_ctl(epollFD_, EPOLL_CTL_DEL, eventFD_, NULL);
  VLOG(2) << "FilerCtx: epoll_ctl retcode: " << retcode << std::endl;
  assert(retcode >= 0);
  */
  retcode = close(eventFD_);
  if (retcode < 0) {
    LOG(ERROR) << "Failed to close fd=" << eventFD_ << " errno=" << -errno;
  }
  retcode = io_queue_release(ioCtx_);
  if (retcode < 0) {
    LOG(ERROR) << "Failed to release ioctx errno=" << retcode;
  }
  (void)retcode;
}

std::string FilerCtx::getState() const {
  std::ostringstream s;
  // json format
  s << " \"ctx\":{\"numAvail\":" << numAvailable_ << ",\"queueDepth\":" << ioQueueDepth_ << "}";
  return s.str();
}

// ============

IOExecutor::Config IOExecutor::defaultConfig_;

IOExecutor::Config::Config() { setDerivedParam(); }

IOExecutor::Config::Config(uint32_t queueDepth) : queueDepth_(queueDepth) {
  setDerivedParam();
}

void IOExecutor::Config::setDerivedParam() {
  maxRequestQueueSize_ = queueDepth_ / 5;
  maxFdQueueSize_ = queueDepth_;
}

void IOExecutor::Config::print() const {
  LOG(INFO) << " \"queueDepth\":" << queueDepth_
            << ",\"maxRequestQueueSize\":" << maxRequestQueueSize_
            << ",\"maxFdQueueSize\":" << maxFdQueueSize_
            << ",\"minSubmitSize\":" << minSubmitSize_
            << ",\"noSubmitterThread\":" << noSubmitterThread_;
}

namespace po = boost::program_options;

int
IOExecutor::Config::addOptions(boost::program_options::options_description& desc) {

  po::options_description ioexecOptions("ioexec config");

  ioexecOptions.add_options()
    ("ioexec.ctx_queue_depth", 
      po::value<uint32_t>(&queueDepth_), 
      "io depth of each context in IOExecutor")
    ("ioexec.cpu_core", 
      po::value<std::vector<CoreId>>(&cpuCores_)->multitoken(),
      "cpu cores dedicated to IO");

  desc.add(ioexecOptions);

  return 0;
}

// ================

void IOExecutor::Statistics::incrementOps(FilerJob *job) {

  if (job->op_ == FileOp::Write) {

    assert(job->size_);
    write_.numOps_ ++;
    write_.numBytes_ += job->size_;
    write_.waitTime_ = job->waitTime();
    write_.serviceTime_ = job->serviceTime();
    write_.waitHist_ = job->waitTime();
    write_.serviceHist_ = job->serviceTime();

  } else if (job->op_ == FileOp::NonAlignedWrite) {

    assert(job->size_);
    nonAlignedWrite_.numOps_ ++;
    nonAlignedWrite_.numBytes_ += job->size_;
    nonAlignedWrite_.waitTime_ = job->waitTime();
    nonAlignedWrite_.serviceTime_ = job->serviceTime();
    nonAlignedWrite_.waitHist_ = job->waitTime();
    nonAlignedWrite_.serviceHist_ = job->serviceTime();

  } else if (job->op_ == FileOp::Read) {

    assert(job->size_);
    read_.numOps_ ++;
    read_.numBytes_ += job->size_;
    read_.waitTime_ = job->waitTime();
    read_.serviceTime_ = job->serviceTime();
    read_.waitHist_ = job->waitTime();
    read_.serviceHist_ = job->serviceTime();

  } else if (job->op_ == FileOp::Delete) {

    delete_.numOps_ ++;
    //delete_.numBytes_ += job->size_; not increment
    delete_.waitTime_ = job->waitTime();
    delete_.serviceTime_ = job->serviceTime();
    delete_.waitHist_ = job->waitTime();
    delete_.serviceHist_ = job->serviceTime();
  }

  numCompleted_++;
}

void IOExecutor::Statistics::print() const {
  LOG(INFO) << "\"completionThread\":" << completionThread_.ToString();
  LOG(INFO) << "\"submitterThread\":" << submitterThread_.ToString();
  LOG(INFO) << "\"fdQueueThread\":" << submitterThread_.ToString();

  LOG(INFO) << getState();

  if ((numSubmitted_ != numQueued_) || (numSubmitted_ != numCompleted_)) {
    LOG(ERROR) << "NOTE discrepancy in IOExecutor stats "
                  "between numQueued, numSubmitted and numCompleted";
  }
}

std::string IOExecutor::Statistics::getState() const {
  std::ostringstream s;

  // json format
  s << "{\"stats\":{"
    << "\"write\":"   << write_.getState() 
    << ",\"nonAlignedWrite\":"   << nonAlignedWrite_.getState() 
    << ",\"read\":"    << read_.getState() 
    << ",\"delete\":"  << delete_.getState() 
    << ",\"numQueued\":" << numQueued_
    << ",\"numSubmitted\":" << numSubmitted_ 
    << ",\"numCompleted\":" << numCompleted_
    << ",\"maxRequestQueueSize\":" << maxRequestQueueSize_
    << ",\"maxFdQueueSize\":" << maxFdQueueSize_
    << ",\"idleLoop\":" << idleLoop_
    << ",\"numProcessedInLoop\":" << numProcessedInLoop_
    << ",\"numCompletionEvents\":" << numCompletionEvents_
    << ",\"requestQueueLow1\":" << requestQueueLow1_
    << ",\"requestQueueLow2\":" << requestQueueLow2_
    << ",\"requestQueueFull\":" << requestQueueFull_
    << "}}";

  return s.str();
}

std::string IOExecutor::Statistics::OpStats::getState() const {
  std::ostringstream s;

  // json format
  s << " {\"numOps\":" << numOps_ 
    << ",\"numBytes\":" << numBytes_
    << ",\"waitTime\":" << waitTime_ 
    << ",\"waitHist\":" << waitHist_
    << ",\"serviceTime\":" << serviceTime_ 
    << ",\"serviceHist\":" << serviceHist_
    << "}";

  return s.str();
}

// ===================

IOExecutor::IOExecutor(const std::string &name, CoreId core,
                       const Config &config)
    : Executor(name, core), config_(config), requestQueue_(config.queueDepth_),
      fdQueue_(0) {
  config_.print();

  epollFD_ = epoll_create1(0);
  assert(epollFD_ >= 0);

  ctx_.init(config_.queueDepth_, epollFD_);

  ctxCond_.init(config_.queueDepth_, /*fd*/ 0);
  fdQueueCond_.init(0, /*fd*/ 0);

  completionThreadShutdown_.init(epollFD_);

  state_ = State::RUNNING;

  if (config_.noSubmitterThread_) {
    periodicTimer_.init(epollFD_, 5 /*timer every n sec*/, 0);
  } else {
    submitterThread_ = std::thread(std::bind(&IOExecutor::execute, this));
  }

  try {
    fdQueueThread_ = std::thread(std::bind(&IOExecutor::ProcessFdQueue, this));

    completionThread_ =
        std::thread(std::bind(&IOExecutor::ProcessCompletions, this));

    LOG(INFO) << "IOExecutor started " << name_
              << ":ioexecutor=" << (void *)this << ":core=" << core_;
  } catch (const std::exception &e) {
    LOG(ERROR) << "Unable to start threads. Exception=" << e.what();
    state_ = State::NOT_STARTED;
  }
}

IOExecutor::~IOExecutor() {
  if (state_ != State::TERMINATED) {
    stop();
  }
}

void IOExecutor::execute() {
  if (core_ > CoreIdInvalid) {
    gobjfs::os::BindThreadToCore(core_);
  }
  // gobjfs::RaiseThreadPriority();

  LOG(INFO) << "IOExecutor started " << name_ << ":ioexecutor=" << (void *)this
            << ":core=" << core_ << ":submitter threadid=" << gettid();

  minSubmitSize_ = config_.minSubmitSize_;

  while (state_ != NO_MORE_INTAKE) {
    int32_t numProcessedInLoop = 0;

    if (requestQueueSize_ >= (int32_t)minSubmitSize_) {
      numProcessedInLoop += ProcessRequestQueue();
      minSubmitSize_ = config_.minSubmitSize_;
    } else {
      // gradually reduce barrier to entry for new requests
      if (minSubmitSize_ > 1)
        minSubmitSize_ /= 2;
    }

    stats_.numProcessedInLoop_ = numProcessedInLoop;

    // if no work done in this round, save some CPU
    if (numProcessedInLoop == 0) {
      stats_.idleLoop_++;
      // sleep if no ctx and no requests for 5 consecutive loops
      {
        // if no work, lets wait
        if ((requestQueueSize_ < (int32_t)minSubmitSize_) &&
            (fdQueueSize_ == 0)) {
          std::unique_lock<std::mutex> lck(submitterCond_.mutex_);
          int32_t sleepTime = 1;

          while ((requestQueueSize_ < (int32_t)minSubmitSize_) &&
                 (fdQueueSize_ == 0) && (state_ == RUNNING)) {
            if (minSubmitSize_ > 1)
              minSubmitSize_ /= 2;

            submitterWaitingForNewRequests_ = true;
            if (sleepTime == 1) {
              stats_.requestQueueLow1_++;
            } else {
              stats_.requestQueueLow2_++;
            }
            submitterCond_.cond_.wait_for(lck,
                                          std::chrono::microseconds(sleepTime));
            if (sleepTime < 1000)
              sleepTime *= 10;
            LOG_EVERY_N(INFO, 1000) // log every 10 sec or so
                << "waiting with requestQueueSize=" << requestQueueSize_
                << ":fdQueueSize=" << fdQueueSize_
                << ":idleloop=" << stats_.idleLoop_
                << ":minSubmitSize=" << minSubmitSize_
                << ":numReads=" << stats_.read_.numOps_
                << ":numWrites=" << stats_.write_.numOps_ 
                << ":numNonAlignedWrites=" << stats_.nonAlignedWrite_.numOps_ 
                << ":state=" << state_;
            submitterWaitingForNewRequests_ = false;
          }
        }
      }
    }
  }

  stats_.submitterThread_.getThreadStats();
}

void IOExecutor::stop() {
  state_ = State::NO_MORE_INTAKE;

  if (!config_.noSubmitterThread_) {
    submitterCond_.wakeup();
    ctxCond_.wakeup();
    try {
      submitterThread_.join();
    } catch (const std::exception &e) {
      LOG(ERROR) << "Failed to join submitterThread. Exception=" << e.what();
    }
  }

  fdQueueCond_.wakeup();
  try {
    fdQueueThread_.join();
  } catch (const std::exception &e) {
    LOG(ERROR) << "Failed to join fdQueueThread. Exception=" << e.what();
  }

  state_ = State::FINAL_SHUTDOWN;

  completionThreadShutdown_.send();
  try {
    completionThread_.join();
  } catch (const std::exception &e) {
    LOG(ERROR) << "Failed to join completionThread. Exception=" << e.what();
  }

  stats_.print();

  close(epollFD_);

  state_ = State::TERMINATED;
}

int32_t IOExecutor::ProcessRequestQueue() {
  int32_t numToSubmit = 0;

  iocb *post_iocb[ctx_.ioQueueDepth_];
  // post_iocb can be freed after io_submit()

  // as per linux source code, io_submit() makes
  // a copy of iocb [using copy_from_user() in fs/aio.c]
  // that is why we can allocate iocb array on stack
  // and free it after io_submit
  iocb cbVec[ctx_.ioQueueDepth_];

  ctxCond_.pause();  // dummy increment to check if ctx available
  ctxCond_.wakeup(); // undo decrement

  while (!ctx_.isEmpty()) {
    const bool gotJob = requestQueue_.consume_one([&](FilerJob *job) {
      iocb *cb = &cbVec[numToSubmit];

      job->prepareCallblock(cb);

      io_set_eventfd(cb, ctx_.eventFD_);
      post_iocb[numToSubmit] = cb;

    });

    if (gotJob) {
      numToSubmit++;
      ctx_.decrementNumAvailable(1);
      ctxCond_.pause();
      int32_t num = requestQueueSize_--;
      assert(num >= 0);
      (void)num;
    } else {
      break;
    }
  }

  int32_t numRemaining = numToSubmit;

  if (numToSubmit) {
    VLOG(1) << "to submit num io=" << numToSubmit;

    // how many times do we try to resubmit io
    int32_t numTries = 5;

    while (numTries--) {

      assert(numRemaining > 0);

      int iosubmitRetcode = io_submit(ctx_.ioCtx_, numRemaining,
                                      &post_iocb[numToSubmit - numRemaining]);

      /*
       * if errcode < 0
       *   if errcode = -EINTR/-EAGAIN
       *     retry
       *   else
       *     abort
       * else
       *   if not all ios submitted
       *     resubmit the remaining in next batch
       *   else
       *     done
       */

      if (iosubmitRetcode < 0) {

        if ((iosubmitRetcode == -EINTR) || (iosubmitRetcode == -EAGAIN)) {
          continue;
        }
        // if you come here, its because fd is invalid OR
        // offset or buffer is not 512 aligned
        std::ostringstream ostr;

        // abort all remaining iocb
        for (int32_t idx = numToSubmit - numRemaining; idx < numToSubmit;
             idx++) {
          iocb *cb = post_iocb[idx];
          if (!cb) {
            ostr << ":iocb at index=" << idx << " became null";
          } else {
            FilerJob *job = static_cast<FilerJob *>(cb->data);
            if (job) {
              bool isValidRet = job->isValid(ostr);
              (void)isValidRet; // ignore
              job->retcode_ = iosubmitRetcode;
              doPostProcessingOfJob(job);
            } else {
              LOG(ERROR) << "found cb data to be null for cb=" << (void *)cb;
            }
          }
        }

        LOG(ERROR) << "Failed io_submit " << numRemaining
                   << " got errno=" << iosubmitRetcode
                   << " with errors=" << ostr.str();

        ctx_.incrementNumAvailable(numRemaining);
        ctxCond_.wakeup(numRemaining);
        break;

      } else {
        // only increment as many as are submitted
        int32_t numSubmitted = iosubmitRetcode;

        stats_.numSubmitted_ += numSubmitted;
        numRemaining -= numSubmitted;

        if (numRemaining > 0) {
          LOG(WARNING) << "Round=" << numTries << " submitted " << numSubmitted
                       << " out of " << numRemaining + numSubmitted
                       << " total size " << numToSubmit;

          ctx_.incrementNumAvailable(numSubmitted);
          ctxCond_.wakeup(numSubmitted);
        } else {
          assert(numRemaining == 0);
          // we are done here
          break;
        }
      }
    }

    if (numRemaining) {
      // some IOs could not be submitted after multiple rounds
      // lets return them as unsubmitted
      for (int32_t idx = (numToSubmit - numRemaining); idx < numToSubmit;
           idx++) {
        iocb *cb = post_iocb[idx];
        FilerJob *job = static_cast<FilerJob *>(cb->data);
        if (job) {
          job->retcode_ = -EINVAL;
          doPostProcessingOfJob(job);
        } else {
          LOG(ERROR) << "found cb data to be null for cb=" << (void *)cb;
        }
      }

      ctx_.incrementNumAvailable(numRemaining);
      ctxCond_.wakeup(numRemaining);

      LOG(ERROR) << "only able to submit " << numRemaining << " out of "
                 << numToSubmit;
    }

    if (requestQueueSize_ < (int32_t)config_.maxRequestQueueSize_) {
      requestQueueHasSpace_.wakeup();
    }
  }

  return (numToSubmit - numRemaining);
}

int32_t IOExecutor::ProcessFdQueue() {
  if (core_ > CoreIdInvalid) {
    gobjfs::os::BindThreadToCore(core_);
  }

  LOG(INFO) << "IOExecutor started " << name_ << ":ioexecutor=" << (void *)this
            << ":core=" << core_ << ":fdQueue threadid=" << gettid();

  uint32_t numConsumed = 0;

  while (state_ != NO_MORE_INTAKE) {
    fdQueueCond_.pause();

    bool gotJob = fdQueue_.consume_one([&](FilerJob *job) {

      if (job->op_ == FileOp::Delete) {
        job->setWaitTime();
        stats_.numSubmitted_ ++;
        int retcode = ::unlink(job->fileName_.c_str());
        job->retcode_ = (retcode == 0) ? 0 : -errno;
        if (retcode != 0) {
          LOG(ERROR) << "delete file=" << job->fileName_
                     << " failed errno=" << job->retcode_;
        }
      } else if (job->op_ == FileOp::NonAlignedWrite) {
        job->setWaitTime();
        stats_.numSubmitted_ ++;
        ssize_t writeSz = ::pwrite(job->fd_, job->buffer_, job->userSize_, job->offset_);
        if (writeSz != job->userSize_) {
          job->retcode_ = -errno;
          LOG(ERROR) << "op=" << job->op_ 
            << " failed for job=" << (void *)job
            << " errno=" << job->retcode_;
        } else {
          job->retcode_ = 0;
        }
      } else {
        LOG(ERROR) << "unknown op=" << job->op_ << " for job=" << (void *)job;
        job->retcode_ = -EINVAL;
      }

      doPostProcessingOfJob(job);
    });

    if (gotJob) {
      numConsumed++;
    } else {
      break;
    }

    fdQueueSize_--;
    if (fdQueueSize_ >= (int32_t)config_.maxRequestQueueSize_) {
      fdQueueHasSpace_.wakeup();
    }
  }

  stats_.fdQueueThread_.getThreadStats();
  return 0;
}

int IOExecutor::submitTask(FilerJob *job, bool blocking) {
  int ret = 0;

  do {

    if (state_ != RUNNING) {
      // signal to caller that no more jobs
      LOG(ERROR) << "shutting down. rejecting job=" << (void *)job;
      ret = -EAGAIN;
      break;
    }

    if (!IsDirectIOAligned(job->userSize_)) {
      if (job->op_ == FileOp::Write) {
        job->op_ = FileOp::NonAlignedWrite;
        blocking = true;
      } else if (job->op_ == FileOp::Read) {
        // short reads work with O_DIRECT
      }
    }

    if ((job->op_ == FileOp::Delete) || 
      (job->op_ == FileOp::Sync) ||
      (job->op_ == FileOp::NonAlignedWrite)) {

      if (fdQueueSize_ > (int32_t)config_.maxRequestQueueSize_) {
        if (!blocking) {
          LOG(ERROR) << "FD Queue full.  rejecting nonblocking job=" << (void *)job;
          ret = job->retcode_ = -EAGAIN;
          break;
        } else {
          stats_.requestQueueFull_++;
          fdQueueHasSpace_.pause();
        }
      }

      // increment size before push, to prevent race conditions
      job->setSubmitTime();
      job->executor_ = this;
      stats_.numQueued_++;

      stats_.maxFdQueueSize_ = ++fdQueueSize_;

      bool pushReturn = false;
      do {
        pushReturn = fdQueue_.push(job);
        if (pushReturn == false) {
          LOG_EVERY_N(WARNING, 10) << "push into fdQueue failing";
        }
      } while (pushReturn == false);
      fdQueueCond_.wakeup();
      break;
    }

    else if ((job->op_ == FileOp::Write) || 
      (job->op_ == FileOp::Read)) {

      if (requestQueueSize_ > (int32_t)config_.maxRequestQueueSize_) {
        if (!blocking) {
          LOG(ERROR) << "Async Queue full.  rejecting nonblocking job=" << (void *)job;
          ret = job->retcode_ = -EAGAIN;
          break;
        } else {
          stats_.requestQueueFull_++;
          requestQueueHasSpace_.pause();
        }
      }

      // set FilerJob variables and increment queue size,
      // before pushing into queue
      // otherwise asserts fail because completion thread
      // also changes FilerJob
      stats_.maxRequestQueueSize_ = ++requestQueueSize_;
      job->setSubmitTime();
      job->executor_ = this;
      stats_.numQueued_++;

      bool pushReturn = false;
      do {
        pushReturn = requestQueue_.push(job);
        if (pushReturn == false) {
          LOG_EVERY_N(WARNING, 10) << "push into requestQueue failing";
        }
      } while (pushReturn == false);

      // if context is free & num jobs > min, wakeup

      if (config_.noSubmitterThread_) {
        if (requestQueueSize_ >= (int32_t)minSubmitSize_) {
          std::unique_lock<std::mutex> lck(submitterCond_.mutex_);
          if (requestQueueSize_ >= (int32_t)minSubmitSize_)
            ProcessRequestQueue();
        }
      } else {
        if (submitterWaitingForNewRequests_) {
          std::unique_lock<std::mutex> lck(submitterCond_.mutex_);
          if (submitterWaitingForNewRequests_) {
            submitterCond_.cond_.notify_one();
          }
        }
      }
    } else {
      LOG(ERROR) << "bad op=" << job->op_;
      ret = -EAGAIN;
    }
  } while (0);

  return ret;
}

/**
 * The reason we need to use epoll_wait() is in order to obtain
 * shutdown notification without burning CPU.
 * Otherwise it would be sufficient to loop on io_getevents().
 */
void IOExecutor::ProcessCompletions() {

  if (core_ >= 0) {
    gobjfs::os::BindThreadToCore(core_);
  }

  LOG(INFO) << "IOExecutor started " << name_ << " ioexecutor=" << (void *)this
            << " core=" << core_ << " completion threadid=" << gettid();

  epoll_event readyEpollEvents[EPOLL_MAXEVENT];

  while (1) {
    if ((state_ == FINAL_SHUTDOWN) &&
        (stats_.numQueued_ == stats_.numCompleted_)) {
      // all outstanding IO done. now its safe to exit
      break;
    }

    bzero(readyEpollEvents, sizeof(epoll_event) * EPOLL_MAXEVENT);

    int numEpollEvents = 0;

    do {
      numEpollEvents =
          epoll_wait(epollFD_, readyEpollEvents, EPOLL_MAXEVENT, -1);
    } while (numEpollEvents < 0 && ((errno == EINTR) || (errno == EAGAIN)));

    if (numEpollEvents < 0) {
      LOG(ERROR) << "completions thread got epoll_wait error=" << errno;
      continue;
    }

    assert(numEpollEvents > 0); // should not happen
    stats_.numCompletionEvents_ += numEpollEvents;

    for (int i = 0; i < numEpollEvents; ++i) {
      epoll_event &thisEvent = readyEpollEvents[i];

      if ((thisEvent.events & (EPOLLERR | EPOLLHUP | EPOLLPRI)) ||
          !(thisEvent.events & EPOLLIN)) {

        LOG(WARNING) << " received abnormal event on epoll= "
                     << thisEvent.events;

      } else if (thisEvent.data.ptr == &completionThreadShutdown_) {

        // handle shutdown request
        LOG(INFO) << "completion thread for core=" << core_
                  << " got shutdown request";
        uint64_t counter;
        completionThreadShutdown_.recv(counter);
        // signal was sent to wakeup thread
        if (state_ == State::FINAL_SHUTDOWN) {
          // process jobs stuck in request queue
          std::unique_lock<std::mutex> lck(submitterCond_.mutex_);
          ProcessRequestQueue();
        }

      } else if (thisEvent.data.ptr == &periodicTimer_) {

        if (config_.noSubmitterThread_) {
          // handle timer request
          VLOG(1) << "completion thread for core=" << core_
                  << " got timer request";
          periodicTimer_.recv();
          if (state_ == State::FINAL_SHUTDOWN) {
            // process jobs stuck in request queue
            std::unique_lock<std::mutex> lck(submitterCond_.mutex_);
            ProcessRequestQueue();
          }
        } else {
          LOG(FATAL) << "how did we receive this event?";
        }

      } else if (thisEvent.data.ptr == &ctx_) {

        // find out how many io events are actually available in the eventfd
        FilerCtx *ctxPtr = reinterpret_cast<FilerCtx *>(thisEvent.data.ptr);
        assert(ctxPtr);
        int64_t numEvents = 0;

        ssize_t ret = read(ctxPtr->eventFD_, &numEvents, sizeof(numEvents));

        assert(ret == sizeof(numEvents));
        assert(numEvents > 0 && numEvents <= ctxPtr->ioQueueDepth_);
        {
          // process all available io events from the firing io context
          io_event readyIOEvents[numEvents];
          bzero(readyIOEvents, sizeof(io_event) * numEvents);

          VLOG(1) << "filerctx=" << ctxPtr << " has events=" << numEvents;

          assert(ctxPtr == &ctx_);

          int32_t numProcessedEvents = 0;
          while (numProcessedEvents < numEvents) {

            int numEventsGot = 0;
            do {
              numEventsGot = io_getevents(
                  ctxPtr->ioCtx_, 1, (numEvents - numProcessedEvents),
                  &readyIOEvents[numProcessedEvents], nullptr);
            } while ((numEventsGot == -EINTR) || (numEventsGot == -EAGAIN));

            if (numEventsGot >= 0) {
              numProcessedEvents += numEventsGot;
            } else {
              LOG(ERROR) << "getevents error=" << errno;
            }
          }

          // process the bottom half on all completed jobs in the io context
          ret = ProcessCallbacks(readyIOEvents, numEvents);
          if (ret != 0) {
            // TODO: handle errors
            assert(false);
          }
        }

      } else {
        LOG(ERROR) << "got unknown event with ptr=" << static_cast<void*>(thisEvent.data.ptr);
      }
    }

    if (submitterWaitingForFreeCtx_) {
      std::unique_lock<std::mutex> lck(submitterCond_.mutex_);
      if (submitterWaitingForFreeCtx_ && (!ctx_.isEmpty())) {
        submitterCond_.cond_.notify_one();
      }
    }
  }

  completionThreadShutdown_.destroy();

  stats_.completionThread_.getThreadStats();

  LOG(INFO) << " Completions thread exited "
            << ":ioexecutor=" << (void *)this << ":core=" << core_
            << ":state=" << state_ << ":numQueued=" << stats_.numQueued_
            << ":numSubmitted=" << stats_.numSubmitted_
            << ":numCompleted=" << stats_.numCompleted_
            << ":numEventsProcessed=" << stats_.numCompletionEvents_;

  //google::FlushLogFiles(0); TODO logging
}

int32_t IOExecutor::ProcessCallbacks(io_event *events, int32_t numEvents) {
  int32_t error = 0;

  for (int32_t idx = 0; idx < numEvents; ++idx) {
    // io_event.obj = pointer to iocb, which was submitted in io_submit()
    // io_event.res = on success, it is size of buffer submitted
    //                on failure, it is negative errno
    // io_event.res2 = always seem to be 0 as per test
    // io_event.data = the iocb.data that was set during io_submit()
    FilerJob *job = reinterpret_cast<FilerJob *>(events[idx].data);

    if ((ssize_t)events[idx].res < 0) {
      job->retcode_ = events[idx].res;
      LOG(ERROR) << "IOerror for job=" << (void *)job << ":fd=" << job->fd_
                 << ":op=" << job->op_ << ":size=" << job->size_
                 << ":offset=" << job->offset_ << ":error=" << job->retcode_;
    } else if ((events[idx].res != job->userSize_) 
      && (events[idx].res != job->size_)) {

      job->retcode_ = -EIO;
      LOG(ERROR) << "partial read/write for job=" << (void *)job
                 << ":fd=" << job->fd_ << ":op=" << job->op_
                 << ":expected size=" << job->userSize_
                 << ":actual size=" << events[idx].res
                 << ":offset=" << job->offset_;
    } else {
      job->retcode_ = 0;
    }

    doPostProcessingOfJob(job);
    ctx_.incrementNumAvailable();
    ctxCond_.wakeup();
  }

  return error;
}

int32_t IOExecutor::doPostProcessingOfJob(FilerJob *job) {
  job->reset(); 
  // incrementOps() has to be done after reset() because
  // reset() sets serviceTime , which is used by stats
  stats_.incrementOps(job);
  if (job->closeFileHandle_) {
    close(job->fd_);
  }
  if (job->canBeFreed_) {
    delete job;
  }
  return 0;
}

std::string IOExecutor::getState() const {
  std::ostringstream s;

  s << "{\"core\":" << core_ << ",\"fdqueueSize\":" << fdQueueSize_
    << ",\"requestQueue\":" << requestQueueSize_ << "," << ctx_.getState()
    << "," << stats_.getState() << "}" << std::endl;

  return s.str();
}
} // namespace
