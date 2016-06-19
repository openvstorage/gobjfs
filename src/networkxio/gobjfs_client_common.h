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

#include <mutex>
#include <condition_variable>
#include <iostream>
#include <assert.h>
#include <string>
#include <gobjfs_client.h>

#define ATTRIBUTE_UNUSED __attribute__((unused))

#ifdef FUNC_TRACE 
#ifndef XXEnter
    #define XXEnter() std::cout << "Entering function " << __FUNCTION__ << " , " << __FILE__ << " ( " << __LINE__ << " ) " << std::endl;
#endif
#ifndef XXExit
    #define XXExit() std::cout << "Exiting function " << __FUNCTION__ << " , " << __FILE__ << " ( " << __LINE__ << " ) " << std::endl;
#endif

#ifndef XXDone
    #define XXDone() goto done;
#endif
#else
#define XXEnter()
#define XXExit()
#define XXDone()
#endif


#define GLOG_ERROR(msg) std::cout  << " " << __FUNCTION__ << " , " << __FILE__ << " ( " << __LINE__ << " ) " << msg << std::endl;
#define GLOG_FATAL(msg) GLOG_ERROR(msg)

#ifdef DEBUG_TRACE
#define GLOG_INFO(msg) GLOG_ERROR(msg)
#define GLOG_DEBUG(msg) GLOG_ERROR(msg)
#define GLOG_TRACE(msg) GLOG_ERROR(msg)
#else
#define GLOG_INFO(msg) 
#define GLOG_DEBUG(msg) 
#define GLOG_TRACE(msg) 
#endif

namespace gobjfs { namespace xio {

// TODO
enum class RequestOp
{
    Noop,
    Read,
    Open,
    Close,
};

enum class TransportType
{
    Error,
    SharedMemory,
    TCP,
    RDMA,
};

struct client_ctx_attr
{
    TransportType transport;
    std::string host;
    int port{-1};
};

struct gbuffer
{
    void *buf{nullptr};
    size_t size;
};

struct completion
{
    gcallback complete_cb;
    void *cb_arg{nullptr};
    bool _on_wait{false};
    bool _calling{false};
    bool _signaled{false};
    bool _failed{false};
    ssize_t _rv{0};
    std::condition_variable _cond;
    std::mutex _mutex;
};

struct notifier
{
  private:
    int _count{0};
    std::condition_variable _cond;
    std::mutex _mutex;

  public:
    notifier(int c = 1) : _count(c) {}

    void wait()
    {
      std::unique_lock<std::mutex> l(_mutex);
      _cond.wait(l);
    }

    void wait_for(const timespec* timeout)
    {
      std::unique_lock<std::mutex> l(_mutex);
      _cond.wait_for(
          l, 
          std::chrono::nanoseconds(
            ((uint64_t)timeout->tv_sec * 1000000000) + 
            timeout->tv_nsec));
    }

    /*
    void wait_for(struct timespec* timeout)
    {
      std::unique_lock<std::mutex> l(_mutex);
      _cond.wait_for(
          l_, 
          std::chrono::nanoseconds(
            ((uint64_t)timeout->tv_sec * 1000000000) + 
            timeout->tv_nsec), 
          func);
    }
    */

    void signal()
    {
      // count doesnt need to be std::atomic since its single-threaded here ?
      assert(_count);
      _count --;
      GLOG_DEBUG("signal at count=" << _count);
      if (0 == _count) 
      {
        std::unique_lock<std::mutex> l(_mutex);
        _cond.notify_all();
      }
    }
};

typedef std::shared_ptr<notifier> notifier_sptr;

struct aio_request
{
    struct giocb *giocbp{nullptr};
    completion *cptr{nullptr};
    RequestOp _op{RequestOp::Noop};
    bool _on_suspend{false};
    bool _canceled{false};
    bool _completed{false};
    bool _signaled{false};
    bool _failed{false};
    int _errno{0};
    ssize_t _rv{0};

    notifier_sptr _cvp;
};

}}
