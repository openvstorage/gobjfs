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
#include <boost/thread/lock_guard.hpp>

#include <networkxio/gobjfs_client_common.h>
#include <networkxio/NetworkXioRequest.h>
#include <util/Spinlock.h>

namespace gobjfs { namespace xio
{

MAKE_EXCEPTION(WorkQueueThreadsException);

class NetworkXioWorkQueue
{
public:
    NetworkXioWorkQueue(const std::string& name, EventFD& evfd_)
    : name_(name)
    , nr_threads_(0)
    , nr_queued_work(0)
    , protection_period_(5000)
    , stopping(false)
    , stopped(false)
    , evfd(evfd_)
    {
        XXEnter();
        using namespace std;
        int ret = create_workqueue_threads(thread::hardware_concurrency());
        if (ret < 0)
        {
            throw WorkQueueThreadsException("cannot create worker threads");
        }
        XXExit();
    }

    ~NetworkXioWorkQueue()
    {
        shutdown();
    }

    void
    shutdown()
    {
        if (not stopped)
        {
            stopping = true;
            while (nr_threads_)
            {
              std::unique_lock<std::mutex> lock_(inflight_lock);
              inflight_cond.notify_all();
              ::usleep(500);
            }
            stopped = true;
        }
    }

    void
    work_schedule(NetworkXioRequest *req)
    {
        XXEnter();
        queued_work_inc();
        std::unique_lock<std::mutex> lock_(inflight_lock);
        if (need_to_grow())
        {
            GLOG_DEBUG("nr_threads_ are " << nr_threads_);
            create_workqueue_threads(nr_threads_ * 2);
        }
        inflight_queue.push(req);
        inflight_cond.notify_one();
        XXExit();
    }

    void
    queued_work_inc()
    {
        nr_queued_work++;
    }

    void
    queued_work_dec()
    {
        nr_queued_work--;
    }

    void worker_bottom_half(NetworkXioWorkQueue *wq, NetworkXioRequest *req) {
        XXEnter();
        boost::lock_guard<decltype(finished_lock)> lock_(finished_lock);
        wq->finished.push(req);
        wq->xstop_loop(wq);
        GLOG_DEBUG("Pushed request to finishedqueue" );
        XXExit();
        
    }
    NetworkXioRequest*
    get_finished()
    {
        XXEnter();
        boost::lock_guard<decltype(finished_lock)> lock_(finished_lock);
        NetworkXioRequest *req = finished.front();
        finished.pop();
        XXExit();
        return req;
    }

    bool
    is_finished_empty()
    {
        boost::lock_guard<decltype(finished_lock)> lock_(finished_lock);
        return finished.empty();
    }
private:

    std::string name_;
    std::atomic<size_t> nr_threads_;

    std::condition_variable inflight_cond;
    std::mutex inflight_lock;
    std::queue<NetworkXioRequest*> inflight_queue;

    gobjfs::os::Spinlock finished_lock;
      
    std::queue<NetworkXioRequest*> finished;

    std::atomic<size_t> nr_queued_work;

    std::chrono::steady_clock::time_point thread_life_period_;
    uint64_t protection_period_;

    bool stopping{false};
    bool stopped{false};
    EventFD& evfd;

    void xstop_loop(NetworkXioWorkQueue *wq)
    {
        XXEnter();
        wq->evfd.writefd();
        XXExit();
    }

    std::chrono::steady_clock::time_point
    get_time_point()
    {
        return std::chrono::steady_clock::now();
    }

    size_t
    get_max_wq_depth()
    {
        XXEnter();
        size_t Max_Q_depth = std::thread::hardware_concurrency();
        GLOG_DEBUG("Max WQ Depth = " << Max_Q_depth );
        XXExit();
        return Max_Q_depth;
    }

    bool
    need_to_grow()
    {
        XXEnter();
        if ((nr_threads_ < nr_queued_work) &&
                (nr_threads_ * 2 <= get_max_wq_depth()))
        {
            thread_life_period_ = get_time_point() +
                std::chrono::milliseconds(protection_period_);
            XXExit();
            return true;
        }
        XXExit();
        return false;
    }

    bool
    need_to_shrink()
    {
        XXEnter();
        if (nr_queued_work < nr_threads_ / 2)
        {
            XXExit();
            return thread_life_period_ <= get_time_point();
        }
        thread_life_period_ = get_time_point() +
            std::chrono::milliseconds(protection_period_);
            XXExit();
        return false;
    }

    int
    create_workqueue_threads(size_t nr_threads)
    {

        XXEnter();
        GLOG_DEBUG(" nr_threads and nr_threads_ are " << nr_threads << " , " << nr_threads_ );
        while (nr_threads_ < nr_threads)
        {
            try
            {
                GLOG_DEBUG(" creating worker thread .. " << name_);
                std::thread thr([&](){
                    auto fp = std::bind(&NetworkXioWorkQueue::worker_routine,
                                        this,
                                        std::placeholders::_1);
                    pthread_setname_np(pthread_self(), name_.c_str());
                    fp(this);
                });
                thr.detach();
                nr_threads_++;
            }
            catch (const std::system_error&)
            {
                GLOG_ERROR("cannot create worker thread; created= " << nr_threads_ << " of " << nr_threads);
                return -1;
            }
        }
        GLOG_DEBUG("Now added " << nr_threads_ << " threads " );
        XXExit();
        return 0;
    }


    void
    worker_routine(void *arg)
    {
        XXEnter();
        NetworkXioWorkQueue *wq = reinterpret_cast<NetworkXioWorkQueue*>(arg);

        NetworkXioRequest *req;
        while (true)
        {
            std::unique_lock<std::mutex> lock_(wq->inflight_lock);
            if (wq->need_to_shrink())
            {
                wq->nr_threads_--;
                GLOG_DEBUG("Number of threads shrunk to " << wq->nr_threads_);
                lock_.unlock();
                break;
            }
retry:
            if (wq->inflight_queue.empty())
            {
                wq->inflight_cond.wait(lock_);
                if (wq->stopping)
                {
                    wq->nr_threads_--;
                    break;
                }
                goto retry;
            }
            req = wq->inflight_queue.front();
            wq->inflight_queue.pop();
            lock_.unlock();
            GLOG_DEBUG("Popped request from inflight queue");
            bool finishNow = true;
            if (req->work.func)
            {
              finishNow = req->work.func(&req->work);
            }
            // for sync requests or requests which have error
            // push in finished queue right here
            // response is sent and request gets freed
            if (finishNow) {
                boost::lock_guard<decltype(inflight_lock)> lock_(inflight_lock);
                wq->finished.push(req);
                GLOG_DEBUG("Pushed request to finishedqueue. ReqType is " << (int) req->op);
                xstop_loop(wq);
            }
        }
        XXExit();
    }
};

typedef std::shared_ptr<NetworkXioWorkQueue> NetworkXioWorkQueuePtr;

}} //namespace

