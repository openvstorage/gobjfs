// Copyright 2016 iNuron NV
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef NETWORK_XIO_WORKQUEUE_H_
#define NETWORK_XIO_WORKQUEUE_H_

//#include <youtils/IOException.h>
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
//#include <youtils/SpinLock.h>
#include "NetworkXioRequest.h"

namespace volumedriverfs
{

//MAKE_EXCEPTION(WorkQueueThreadsException, Exception);

class NetworkXioWorkQueue
{
public:
    NetworkXioWorkQueue(const std::string& name, int evfd_)
    : name_(name)
    , nr_threads_(0)
    , nr_queued_work(0)
    , protection_period_(5000)
    , wq_open_sessions_(0)
    , stopping(false)
    , stopped(false)
    , evfd(evfd_)
    {
        XXEnter();
        using namespace std;
        int ret = create_workqueue_threads(thread::hardware_concurrency());
        if (ret < 0)
        {
            std::cout << "Alert !! Create_workquue_threads() failed with error " << std::endl;
            //throw WorkQueueThreadsException("cannot create worker threads");
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
        inflight_lock.lock();
        if (need_to_grow())
        {
            GLOG_DEBUG("nr_threads_ are " << nr_threads_);
            create_workqueue_threads(nr_threads_ * 2);
        }
        inflight_queue.push(req);
        inflight_lock.unlock();
        inflight_cond.notify_one();
        XXExit();
    }

    void
    open_sessions_inc()
    {
        wq_open_sessions_++;
    }

    void
    open_sessions_dec()
    {
        wq_open_sessions_--;
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
        wq->lock_ts(); 
        wq->finished.push(req);
        wq->xstop_loop(wq);
        wq->unlock_ts(); 
        GLOG_DEBUG("Pushed request to finishedqueue" );
        XXExit();
        //wq->finished_lock.unlock();
        
    }
    NetworkXioRequest*
    get_finished()
    {
        XXEnter();
        //boost::lock_guard<decltype(finished_lock)> lock_(finished_lock);
        lock_ts();
        NetworkXioRequest *req = finished.front();
        finished.pop();
        unlock_ts();
        XXExit();
        return req;
    }

    bool
    is_finished_empty()
    {
        //boost::lock_guard<decltype(finished_lock)> lock_(finished_lock);
        // return finished.empty();
        lock_ts();
        bool isEmpty = finished.empty();
        unlock_ts();
        return isEmpty;
    }
private:
    //DECLARE_LOGGER("NetworkXioWorkQueue");

    std::string name_;
    std::atomic<size_t> nr_threads_;

    std::condition_variable inflight_cond;
    std::mutex inflight_lock;
    std::queue<NetworkXioRequest*> inflight_queue;

    //mutable fungi::SpinLock finished_lock;
    std::atomic_flag  finished_lock = ATOMIC_FLAG_INIT;

      
    std::queue<NetworkXioRequest*> finished;

    std::atomic<size_t> nr_queued_work;

    std::chrono::steady_clock::time_point thread_life_period_;
    uint64_t protection_period_;
    std::atomic<uint64_t> wq_open_sessions_;

    bool stopping;
    bool stopped;
    int evfd;

    void xstop_loop(NetworkXioWorkQueue *wq)
    {
        XXEnter();
        xeventfd_write(wq->evfd);
        XXExit();
    }

    inline void lock_ts() {
        XXEnter();
        while (finished_lock.test_and_set(std::memory_order_acquire));
        XXExit();
    }
    
    inline void unlock_ts() {
        XXEnter();
        finished_lock.clear(std::memory_order_release);
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
        size_t Max_Q_depth = std::thread::hardware_concurrency() + 2 * wq_open_sessions_;
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
                //GLOG_ERROR("cannot create worker thread");
                std::cout << " ( " << __FILE__ << " , " << __LINE__ << " ) " << " cannot create worker thread " << std::endl;
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
            if (req->work.func)
            {
                req->work.func(&req->work);
            }
            //wq->finished_lock.lock();
            if ((req->op != NetworkXioMsgOpcode::ReadRsp) && (req->op != NetworkXioMsgOpcode::WriteRsp)){
                wq->lock_ts(); 
                wq->finished.push(req);
                wq->unlock_ts(); 
                GLOG_DEBUG("Pushed request to finishedqueue. ReqType is " << (int) req->op);
            //wq->finished_lock.unlock();
                xstop_loop(wq);
            }
        }
        XXExit();
    }
};

typedef std::shared_ptr<NetworkXioWorkQueue> NetworkXioWorkQueuePtr;

} //namespace

#endif //NETWORK_XIO_WORKQUEUE_H_
