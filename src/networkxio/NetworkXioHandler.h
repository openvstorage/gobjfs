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

#include "common.h"
#include "NetworkXioCommon.h"

namespace gobjfs { namespace xio 
{

inline void
_xio_aio_wake_up_suspended_aiocb(ovs_aio_request *request)
{
    XXEnter();
    GLOG_DEBUG("waking up the suspended thread");
    if (not __sync_bool_compare_and_swap(&request->_on_suspend,
                                         false,
                                         true,
                                         __ATOMIC_RELAXED))
    {
        pthread_mutex_lock(&request->_mutex);
        request->_signaled = true;
        pthread_cond_signal(&request->_cond);
        pthread_mutex_unlock(&request->_mutex);
    }
    XXExit();
}

void
ovs_xio_aio_complete_request(void* opaque, ssize_t retval, int errval)
{
    XXEnter();
    ovs_aio_request *request = reinterpret_cast<ovs_aio_request*>(opaque);
    ovs_completion_t *completion = request->completion;
    RequestOp op = request->_op;
    request->_errno = errval;
    request->_rv = retval;
    request->_failed = (retval == -1 ? true : false);
    request->_completed = true;
    //if (op != RequestOp::AsyncFlush)
    {
        _xio_aio_wake_up_suspended_aiocb(request);
    }
    if (completion)
    {
        completion->_rv = retval;
        completion->_failed = (retval == -1 ? true : false);
       // AioCompletion::get_aio_context().schedule(completion);
    }
    XXExit();
}

void
ovs_xio_complete_request_control(void *opaque, ssize_t retval, int errval)
{
    XXEnter();
    ovs_aio_request *request = reinterpret_cast<ovs_aio_request*>(opaque);
    if (request)
    {
        request->_errno = errval;
        request->_rv = retval;
    }
    XXExit();
}

}} 
