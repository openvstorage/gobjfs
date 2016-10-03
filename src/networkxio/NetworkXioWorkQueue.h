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

#include <iostream>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <atomic>
#include <thread>
#include <chrono>
#include <string>
#include <queue>

#include <networkxio/gobjfs_client_common.h>
#include <networkxio/NetworkXioRequest.h>

namespace gobjfs {
namespace xio {

MAKE_EXCEPTION(WorkQueueThreadsException);

class NetworkXioWorkQueue {
public:
  NetworkXioWorkQueue(const std::string &name, int32_t numCoresForIO)
      : name_(name), nr_threads_(0), nr_queued_work(0),
        protection_period_(5000), stopping(false), stopped(false) {
    XXEnter();
    using namespace std;
    int ret = create_workqueue_threads(numCoresForIO);
    if (ret < 0) {
      throw WorkQueueThreadsException("cannot create worker threads");
    }
    XXExit();
  }

  ~NetworkXioWorkQueue() { shutdown(); }

  void shutdown() {
    if (not stopped) {
      stopping = true;
      while (nr_threads_) {
        inflight_cond.notify_all();
        ::usleep(500);
      }
      stopped = true;
    }
  }

  void work_schedule(NetworkXioRequest *req) {
    XXEnter();
    queued_work_inc();
    std::unique_lock<std::mutex> lock_(inflight_lock);
    if (need_to_grow()) {
      GLOG_INFO("nr_threads_ are " << nr_threads_);
      create_workqueue_threads(nr_threads_ * 2);
    }
    inflight_queue.push(req);
    inflight_cond.notify_one();
    XXExit();
  }

  void queued_work_inc() { nr_queued_work++; }

  void queued_work_dec() { nr_queued_work--; }

private:
  std::string name_;
  std::atomic<size_t> nr_threads_;

  std::condition_variable inflight_cond;
  std::mutex inflight_lock;
  std::queue<NetworkXioRequest *> inflight_queue;


  std::atomic<size_t> nr_queued_work;

  std::chrono::steady_clock::time_point thread_life_period_;
  uint64_t protection_period_;

  bool stopping{false};
  bool stopped{false};

  std::chrono::steady_clock::time_point get_time_point() {
    return std::chrono::steady_clock::now();
  }

  size_t get_max_wq_depth() {
    XXEnter();
    size_t Max_Q_depth = std::thread::hardware_concurrency();
    GLOG_DEBUG("Max WQ Depth = " << Max_Q_depth);
    XXExit();
    return Max_Q_depth;
  }

  bool need_to_grow() {
    XXEnter();
    if ((nr_threads_ < nr_queued_work) &&
        (nr_threads_ * 2 <= get_max_wq_depth())) {
      thread_life_period_ =
          get_time_point() + std::chrono::milliseconds(protection_period_);
      XXExit();
      return true;
    }
    XXExit();
    return false;
  }

  bool need_to_shrink() {
    XXEnter();
    if (nr_queued_work < nr_threads_ / 2) {
      XXExit();
      return thread_life_period_ <= get_time_point();
    }
    thread_life_period_ =
        get_time_point() + std::chrono::milliseconds(protection_period_);
    XXExit();
    return false;
  }

  int create_workqueue_threads(size_t requested_threads) {

    XXEnter();

    while (nr_threads_ < requested_threads) {
      try {
        GLOG_INFO(" creating worker thread .. " << name_);
        std::thread thr([&]() {
          auto fp = std::bind(&NetworkXioWorkQueue::worker_routine, this);
          pthread_setname_np(pthread_self(), name_.c_str());
          fp();
        });
        thr.detach();
        nr_threads_++;
      } catch (const std::system_error &) {
        GLOG_ERROR("cannot create any more worker thread; created "
                   << nr_threads_ << " out of " << requested_threads);
        return -1;
      }
    }
    GLOG_INFO("Requested=" << requested_threads << " workqueue threads. Created " << nr_threads_ << " threads ");
    XXExit();
    return 0;
  }

  void worker_routine() {
    XXEnter();

    NetworkXioRequest *req;
    while (true) {
      std::unique_lock<std::mutex> lock_(inflight_lock);
      if (need_to_shrink()) {
        nr_threads_--;
        GLOG_INFO("Number of threads shrunk to " << nr_threads_);
        lock_.unlock();
        break;
      }
    retry:
      if (inflight_queue.empty()) {
        inflight_cond.wait(lock_);
        if (stopping) {
          nr_threads_--;
          break;
        }
        goto retry;
      }
      req = inflight_queue.front();
      inflight_queue.pop();
      lock_.unlock();
      GLOG_DEBUG("Popped request from inflight queue");
      bool finishNow = true;
      if (req->work.func) {
        finishNow = req->work.func(&req->work);
      }
      // for sync requests or requests which have error
      // push in finished queue right here
      // response is sent and request gets freed
      if (finishNow) {
        req->pClientData->worker_bottom_half(req);
      }
    }
    XXExit();
  }
};

typedef std::shared_ptr<NetworkXioWorkQueue> NetworkXioWorkQueuePtr;
}
} // namespace
