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

#include "NetworkXioClient.h"

struct ovs_context_t
{
    TransportType transport;
    std::string host;
    int port;
    std::string uri;
    gobjfs::xio::NetworkXioClientPtr net_client_;
};


inline 
ovs_aio_request* create_new_request(RequestOp op,
                                    struct ovs_aiocb *aio,
                                    notifier_sptr cvp,
                                    ovs_completion_t *completion)
{
    try
    {
        ovs_aio_request *request = new ovs_aio_request;
        request->_op = op;
        request->ovs_aiocbp = aio;
        request->completion = completion;
        /*cnanakos TODO: err handling */
        request->_on_suspend = false;
        request->_canceled = false;
        request->_completed = false;
        request->_signaled = false;
        request->_rv = 0;
        request->_cvp = cvp;
        if (aio and op != RequestOp::Noop)
        {
            aio->request_ = request;
        }
        return request;
    }
    catch (const std::bad_alloc&)
    {
        return NULL;
    }
}

inline int
ovs_xio_open_device(ovs_ctx_t *ctx)
{
    XXEnter();
    ssize_t r;
    struct ovs_aiocb aio;

    auto cvp = std::make_shared<notifier>();

    ovs_aio_request *request = create_new_request(RequestOp::Open,
                                                  &aio,
                                                  cvp,
                                                  NULL);
    if (request == NULL)
    {
        errno = ENOMEM;
        XXExit();
        return -1;
    }

    try
    {
        ctx->net_client_->xio_send_open_request(
          reinterpret_cast<void*>(request));
    }
    catch (const std::bad_alloc&)
    {
        errno = ENOMEM;
        XXExit();
        return -1;
    }
    catch (...)
    {
        errno = EIO;
        XXExit();
        return -1;
    }

    if ((r = ovs_aio_suspend(ctx, &aio, NULL)) < 0)
    {
        GLOG_ERROR("ovs_aio_suspend() failed with error ");
        XXExit();
        return r;
    }
    r = ovs_aio_return(ctx, &aio);
    if (r < 0) {
        GLOG_ERROR("ovs_aio_return() failed with error ");
    }
    if (ovs_aio_finish(ctx, &aio) < 0){
        GLOG_ERROR("ovs_aio_finish() failed with error ");
        r = -1;
    }
    XXExit();
    return r;
}
