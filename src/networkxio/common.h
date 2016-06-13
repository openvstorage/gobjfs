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
#define GLOG_INFO(msg) GLOG_ERROR(msg)
#define GLOG_DEBUG(msg) GLOG_ERROR(msg)
#define GLOG_TRACE(msg) GLOG_ERROR(msg)

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

struct ovs_context_attr_t
{
    TransportType transport;
    char *host{nullptr};
    int port{-1};
};

struct ovs_buffer
{
    void *buf{nullptr};
    size_t size;
};

struct ovs_completion
{
    ovs_callback_t complete_cb;
    void *cb_arg{nullptr};
    bool _on_wait{false};
    bool _calling{false};
    bool _signaled{false};
    bool _failed{false};
    ssize_t _rv{0};
    pthread_cond_t _cond;
    pthread_mutex_t _mutex;
};

struct ovs_aio_request
{
    struct ovs_aiocb *ovs_aiocbp{nullptr};
    ovs_completion_t *completion{nullptr};
    RequestOp _op{RequestOp::Noop};
    bool _on_suspend{false};
    bool _canceled{false};
    bool _completed{false};
    bool _signaled{false};
    bool _failed{false};
    int _errno{0};
    ssize_t _rv{0};
    pthread_cond_t _cond;
    pthread_mutex_t _mutex;
};

