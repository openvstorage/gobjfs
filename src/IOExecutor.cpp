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

int32_t FilerCtx::init(int32_t queueDepth) {
  ioQueueDepth_ = queueDepth;

  int retcode = 0;

  do {
    bzero(&ioCtx_, sizeof(ioCtx_));
    retcode = io_queue_init(queueDepth, &ioCtx_);
    if (retcode != 0) {
      LOG(ERROR) << "Failed to init io queue errno=" << -errno;
      break;
    }
    numAvailable_ = ioQueueDepth_;

  } while (0);

  return retcode;
}

FilerCtx::~FilerCtx() {

  int retcode = io_queue_release(ioCtx_);
  if (retcode < 0) {
    LOG(ERROR) << "Failed to release ioctx errno=" << retcode;
  }
  (void)retcode;
}

std::string FilerCtx::getState() const {
  std::ostringstream s;
  // json format
  s << " \"ctx\":{\"numAvail\":" << numAvailable_
    << ",\"queueDepth\":" << ioQueueDepth_ << "}";
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

int IOExecutor::Config::addOptions(
    boost::program_options::options_description &desc) {

  po::options_description ioexecOptions("ioexec config");

  ioexecOptions.add_options()("ioexec.ctx_queue_depth",
                              po::value<uint32_t>(&queueDepth_),
                              "io depth of each context in IOExecutor")(
      "ioexec.cpu_core",
      po::value<std::vector<CoreId>>(&cpuCores_)->multitoken(),
      "cpu cores dedicated to IO");

  desc.add(ioexecOptions);

  return 0;
}

// ================

void IOExecutor::Statistics::incrementOps(FilerJob *job) {

  if (job->op_ == FileOp::Read) {

    assert(job->size_);
    read_.numOps_++;
    read_.numBytes_ += job->size_;
    read_.waitTime_ = job->waitTime();
    read_.serviceTime_ = job->serviceTime();
    read_.waitHist_ = job->waitTime();
    read_.serviceHist_ = job->serviceTime();

  } else {
	assert("bad_op" == 0);
  }

  numCompleted_++;
}

void IOExecutor::Statistics::print() const {

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
    << ",\"read\":" << read_.getState() 
    << ",\"numCompleted\":" << numCompleted_
    << ",\"maxRequestQueueSize\":" << maxRequestQueueSize_
    << ",\"minSubmitSize\":" << minSubmitSize_
    << ",\"numProcessedInLoop\":" << numProcessedInLoop_
    << ",\"numCompletionEvents\":" << numCompletionEvents_
    << ",\"numInlineFlushes\":" << numInlineFlushes_ 
    << ",\"numExternalFlushes\":" << numExternalFlushes_ 
    << ",\"numCompletionFlushes\":" << numCompletionFlushes_ 
    << ",\"numTimesCtxEmpty\":" << numTimesCtxEmpty_
    << ",\"requestQueueFull\":" << requestQueueFull_ << "}}";

  return s.str();
}

std::string IOExecutor::Statistics::OpStats::getState() const {
  std::ostringstream s;

  // json format
  s << " {\"numOps\":" << numOps_ << ",\"numBytes\":" << numBytes_
    << ",\"waitTime\":" << waitTime_ << ",\"waitHist\":" << waitHist_
    << ",\"serviceTime\":" << serviceTime_
    << ",\"serviceHist\":" << serviceHist_ << "}";

  return s.str();
}

// ===================

IOExecutor::IOExecutor(const std::string &name, CoreId core,
                       const Config &config)
    : Executor(name, core), config_(config), requestQueue_(config.queueDepth_) {
  config_.print();

  setMinSubmitSize(config_.minSubmitSize_);

  ctx_.init(config_.queueDepth_);

  state_ = State::RUNNING;

  LOG(INFO) << "IOExecutor started " << name_
            << ":ioexecutor=" << (void *)this << ":core=" << core_;
}

IOExecutor::~IOExecutor() {
  if (state_ != State::TERMINATED) {
    stop();
  }
}

void IOExecutor::stop() {

  stats_.print();

  state_ = State::TERMINATED;
}

void IOExecutor::setMinSubmitSize(size_t minSubmitSz)  {
  if (minSubmitSz > 0) {
    minSubmitSize_ = minSubmitSz;
    stats_.minSubmitSize_ = minSubmitSz;
	}
}

void IOExecutor::execute() {
  assert(0);
}

int32_t IOExecutor::ProcessRequestQueue(CallType calledFrom) {
  int32_t numToSubmit = 0;

  iocb *post_iocb[ctx_.ioQueueDepth_];
  // post_iocb can be freed after io_submit()

  // as per linux source code, io_submit() makes
  // a copy of iocb [using copy_from_user() in fs/aio.c]
  // that is why we can allocate iocb array on stack
  // and free it after io_submit
  iocb cbVec[ctx_.ioQueueDepth_];

  if (ctx_.isEmpty()) {
    // lets see if we can get any free ctx
    stats_.numTimesCtxEmpty_ ++;
    handleXioEvent(-1, 0, nullptr);
  }

  while (!ctx_.isEmpty()) {
    const bool gotJob = requestQueue_.consume_one([&](FilerJob *job) {
      iocb *cb = &cbVec[numToSubmit];

      job->prepareCallblock(cb);

      //io_set_eventfd(cb, ctx_.eventFD_);
      assert(job->completionFd_ != -1);
      io_set_eventfd(cb, job->completionFd_);
      post_iocb[numToSubmit] = cb;

    });

    if (gotJob) {
      numToSubmit++;
      ctx_.decrementNumAvailable(1);
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
        break;

      } else {
        // only increment as many as are submitted
        int32_t numSubmitted = iosubmitRetcode;

        stats_.numSubmitted_ += numSubmitted;
        stats_.numProcessedInLoop_ = numSubmitted;
        numRemaining -= numSubmitted;
        if (calledFrom == CallType::EXTERNAL) {
          stats_.numExternalFlushes_ ++;
        } else if (calledFrom == CallType::INLINE) {
          stats_.numInlineFlushes_ ++;
        } else if (calledFrom == CallType::COMPLETION) {
          stats_.numCompletionFlushes_ ++;
        }


        if (numRemaining > 0) {
          LOG(WARNING) << "Round=" << numTries << " submitted " << numSubmitted
                       << " out of " << numRemaining + numSubmitted
                       << " total size " << numToSubmit;

          ctx_.incrementNumAvailable(numSubmitted);
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

      LOG(ERROR) << "only able to submit " << numRemaining << " out of "
                 << numToSubmit;
    }

    if (requestQueueSize_ < (int32_t)config_.maxRequestQueueSize_) {
      //requestQueueHasSpace_.wakeup();
    }
  }

  return (numToSubmit - numRemaining);
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


    if ((job->op_ == FileOp::Write) || (job->op_ == FileOp::Read)) {

      if (requestQueueSize_ > (int32_t)config_.maxRequestQueueSize_) {
        if (!blocking) {
          LOG(ERROR) << "Async Queue full.  rejecting nonblocking job="
                     << (void *)job;
          ret = job->retcode_ = -EAGAIN;
          break;
        } else {
          stats_.requestQueueFull_++;
          //requestQueueHasSpace_.pause();
          assert("not supported" == 0); // TODO XIO
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

	    if (requestQueueSize_ > minSubmitSize_
         && ctx_.numAvailable_ > minSubmitSize_) {
      	ProcessRequestQueue(CallType::INLINE);
      }
    } else {
      LOG(ERROR) << "bad op=" << job->op_;
      ret = -EAGAIN;
    }
  } while (0);

  return ret;
}

int IOExecutor::handleXioEvent(int fd, int events, void* data) {

  if (ctx_.isFull()) { 
    return 0;
  }

  io_event readyIOEvents[EPOLL_MAXEVENT];
  bzero(readyIOEvents, sizeof(io_event) * EPOLL_MAXEVENT);

  int numEventsGot = 0;

  //struct timespec ts = {0, 10000 };
  do {
    numEventsGot = io_getevents(
        ctx_.ioCtx_, 1, EPOLL_MAXEVENT,
        &readyIOEvents[0], nullptr);
  } while ((numEventsGot == -EINTR) || (numEventsGot == -EAGAIN));

  if (numEventsGot > 0) {
    stats_.numCompletionEvents_ += numEventsGot;
    // process the bottom half on all completed jobs in the io context
    int ret = ProcessCallbacks(readyIOEvents, numEventsGot);
    if (ret != 0) {
      // TODO: handle errors
      assert(false);
    }
    // if enuf ctx got freed up, see if we can submit more IO
    if (requestQueueSize_ > minSubmitSize_
       && ctx_.numAvailable_ > minSubmitSize_) {
      ProcessRequestQueue(CallType::COMPLETION);
    }
  } else if (numEventsGot < 0) {
    LOG(ERROR) << "getevents error=" << errno;
  }

  return 0;
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
    } else if ((events[idx].res != job->userSize_) &&
               (events[idx].res != job->size_)) {

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

  s << "{" << ctx_.getState()
    << "," << stats_.getState() << "}" << std::endl;

  return s.str();
}
} // namespace
