// This file is dual licensed GPLv2 and Apache 2.0.
// Active license depends on how it is used.
//
// Copyright 2016 iNuron NV
//
// // GPL //
// This file is part of OpenvStorage.
//
// OpenvStorage is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with OpenvStorage. If not, see <http://www.gnu.org/licenses/>.
//
// // Apache 2.0 //
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cerrno>
#include <limits.h>
#include <map>

#include "volumedriver.h"
#include "common.h"
#include "NetworkXioHandler.h"
#include "NetworkXioCommon.h"
#include "context.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#ifdef __GNUC__
#define likely(x)       __builtin_expect(!!(x), 1)
#define unlikely(x)     __builtin_expect(!!(x), 0)
#else
#define likely(x)       (x)
#define unlikely(x)     (x)
#endif

ovs_ctx_attr_t*
ovs_ctx_attr_new()
{
    try
    {
        ovs_ctx_attr_t *attr = new ovs_ctx_attr_t;
        attr->transport = TransportType::Error;
        attr->host = NULL;
        attr->port = 0;
        return attr;
    }
    catch (const std::bad_alloc&)
    {
        errno = ENOMEM;
    }
    return NULL;
}

int
ovs_ctx_attr_destroy(ovs_ctx_attr_t *attr)
{
    if (attr)
    {
        if (attr->host)
        {
            free(attr->host);
        }
        delete attr;
    }
    return 0;
}

int
ovs_ctx_attr_set_transport(ovs_ctx_attr_t *attr,
                           const char *transport,
                           const char *host,
                           int port)
{
    if (attr == NULL)
    {
        errno = EINVAL;
        return -1;
    }


    if ((not strcmp(transport, "tcp")) and host)
    {
        attr->transport = TransportType::TCP;
        attr->host = strdup(host);
        attr->port = port;
        return 0;
    }

    if ((not strcmp(transport, "rdma")) and host)
    {
        attr->transport = TransportType::RDMA;
        attr->host = strdup(host);
        attr->port = port;
        return 0;
    }
    errno = EINVAL;
    return -1;
}

ovs_ctx_t*
ovs_ctx_new(const ovs_ctx_attr_t *attr)
{
    ovs_ctx_t *ctx = NULL;

    if(not attr)
    {
        errno = EINVAL;
        return NULL;
    }

    if (attr->transport != TransportType::TCP &&
        attr->transport != TransportType::RDMA)
    {
        errno = EINVAL;
        return NULL;
    }

    try
    {
        ctx = new ovs_ctx_t;
        ctx->transport = attr->transport;
        ctx->host = std::string(attr->host ? attr->host : "");
        ctx->port = attr->port;
        ctx->net_client_ = nullptr;
    }
    catch (const std::bad_alloc&)
    {
        errno = ENOMEM;
        return NULL;
    }

    switch (ctx->transport)
    {
    case TransportType::TCP:
        ctx->uri = "tcp://" + ctx->host + ":" + std::to_string(ctx->port);
        break;
    case TransportType::RDMA:
        ctx->uri = "rdma://" + ctx->host + ":" + std::to_string(ctx->port);
        break;
    case TransportType::SharedMemory: 
    case TransportType::Error: /* already catched */
        errno = EINVAL;
        return NULL;
    }
    return ctx;
}

int
ovs_ctx_init(ovs_ctx_t *ctx,
             const char* dev_name,
             int oflag)
{
    int err = 0;    
    XXEnter();
    if (oflag != O_RDONLY &&
        oflag != O_WRONLY &&
        oflag != O_RDWR) {
        err = -EINVAL;
        XXDone();
    }

    ctx->oflag = oflag;
    ctx->dev_name = std::string(dev_name);
    if (ctx->transport == TransportType::RDMA ||
             ctx->transport == TransportType::TCP)
    {
        try
        {
            ctx->net_client_ =
                std::make_shared<gobjfs::xio::NetworkXioClient>(ctx->uri);
        }
        catch (...)
        {
            XXExit();
            err = -EIO;
            XXDone();
        }
        err = ovs_xio_open_device(ctx, dev_name);
        if (err < 0) {
            GLOG_ERROR("ovs_xio_open_device failed with error " << err);
            XXDone();
        }
    }
done:
    XXExit();
    return err;
}

int
ovs_ctx_destroy(ovs_ctx_t *ctx)
{
    int r = 0;
    if (ctx == NULL)
    {
        errno = EINVAL;
        return (r = -1);
    }

    if (ctx->net_client_)
    {
        ctx->net_client_.reset();
    }
    delete ctx;
    return r;
}

static int
_ovs_submit_aio_request(ovs_ctx_t *ctx,
                        const std::string& filename,
                        struct ovs_aiocb *ovs_aiocbp,
                        ovs_completion_t *completion,
                        const RequestOp& op)
{
    XXEnter();
    int r = 0, accmode;
    gobjfs::xio::NetworkXioClientPtr net_client = ctx->net_client_;


    if (ctx == NULL || ovs_aiocbp == NULL)
    {
        errno = EINVAL;
        XXExit();
        return -1;
    }

    if ((ovs_aiocbp->aio_nbytes <= 0 ||
         ovs_aiocbp->aio_offset < 0))
    {
        errno = EINVAL;
        XXExit();
        return -1;
    }

    accmode = ctx->oflag & O_ACCMODE;
    switch (op)
    {
    case RequestOp::Read:
        if (accmode == O_WRONLY)
        {
            errno = EBADF;
            XXExit();
            return -1;
        }
        break;
    default:
        errno = EBADF;
        XXExit();
        return -1;
    }

    ovs_aio_request *request = create_new_request(op, 
                                                  ovs_aiocbp, 
                                                  completion);
    if (request == NULL)
    {
        GLOG_ERROR("create_new_request() failed \n");
        errno = ENOMEM;
        XXExit();
        return -1;
    }

    switch (op)
    {
    case RequestOp::Read:
        {
            try
            {
                net_client->xio_send_read_request(filename,
                                                  ovs_aiocbp->aio_buf,
                                                  ovs_aiocbp->aio_nbytes,
                                                  ovs_aiocbp->aio_offset,
                                                  reinterpret_cast<void*>(request));
            }
            catch (const std::bad_alloc&)
            {
                errno = ENOMEM; r = -1;
                GLOG_ERROR("xio_send_read_request() failed \n");
            }
            catch (...)
            {
                errno = EIO; r = -1;
                GLOG_ERROR("xio_send_read_request() failed \n");
            }
        }
        break;
    default:
        errno = EINVAL; r = -1;
        GLOG_ERROR("incorrect command \n");
        break;
    }
    if (r < 0)
    {
        delete request;
    }
    int saved_errno = errno;
    errno = saved_errno;
    if (r != 0) {
        GLOG_ERROR(" Remove request send failed with error " << r );
    }
    return r;
}

int
ovs_aio_read(ovs_ctx_t *ctx,
             const std::string& filename,
             struct ovs_aiocb *ovs_aiocbp)
{
    XXEnter();
    int err = _ovs_submit_aio_request(ctx,
                                      filename,
                                       ovs_aiocbp,
                                       NULL,
                                       RequestOp::Read);
    XXExit();
    return err;
}


int
ovs_aio_error(ovs_ctx_t *ctx,
              struct ovs_aiocb *ovs_aiocbp)
{
    int r = 0;
    if (ctx == NULL || ovs_aiocbp == NULL)
    {
        errno = EINVAL;
        return (r = -1);
    }

    if (ovs_aiocbp->request_->_canceled)
    {
        return (r = ECANCELED);
    }

    if (not ovs_aiocbp->request_->_completed)
    {
        return (r = EINPROGRESS);
    }

    if (ovs_aiocbp->request_->_failed)
    {
        return (r = ovs_aiocbp->request_->_errno);
    }
    else
    {
        return r;
    }
}

ssize_t
ovs_aio_return(ovs_ctx_t *ctx,
               struct ovs_aiocb *ovs_aiocbp)
{
    int r = 0;
    XXEnter();
    if (ctx == NULL || ovs_aiocbp == NULL)
    {
        GLOG_ERROR("ctx or ovs_aiocbp NULL");
        r = -EINVAL;
        XXExit();
        return r;
    }

    errno = ovs_aiocbp->request_->_errno;
    if (not ovs_aiocbp->request_->_failed)
    {
        r = ovs_aiocbp->request_->_rv;
    }
    else
    {
        r = GetNegative(errno);
        GLOG_ERROR("ovs_aiocbp->request_->_failed is true. Error is " << r);
    }
    XXExit();
    return r;
}

int
ovs_aio_finish(ovs_ctx_t *ctx,
               struct ovs_aiocb *ovs_aiocbp)
{
    XXEnter();
    if (ctx == NULL || ovs_aiocbp == NULL)
    {
        errno = EINVAL;
        GLOG_ERROR("ctx or ovs_aiocbp NULL ");
        return -1;
    }

    pthread_cond_destroy(&ovs_aiocbp->request_->_cond);
    pthread_mutex_destroy(&ovs_aiocbp->request_->_mutex);
    delete ovs_aiocbp->request_;
    XXExit();
    return 0;
}

int
ovs_aio_suspend(ovs_ctx_t *ctx,
                ovs_aiocb *ovs_aiocbp,
                const struct timespec *timeout)
{
    XXEnter();
    int r = 0;
    if (ctx == NULL || ovs_aiocbp == NULL)
    {
        errno = EINVAL;
        XXExit();
        return (r = -1);
    }
    if (__sync_bool_compare_and_swap(&ovs_aiocbp->request_->_on_suspend,
                                     false,
                                     true,
                                     __ATOMIC_RELAXED))
    {
        pthread_mutex_lock(&ovs_aiocbp->request_->_mutex);
        while (not ovs_aiocbp->request_->_signaled && r == 0)
        {
            if (timeout)
            {
                GLOG_DEBUG("Now going to wait on _cond ");
                r = pthread_cond_timedwait(&ovs_aiocbp->request_->_cond,
                                           &ovs_aiocbp->request_->_mutex,
                                           timeout);
                GLOG_DEBUG("Woken Up");
            }
            else
            {
                GLOG_DEBUG("Now going to wait on _cond ");
                r = pthread_cond_wait(&ovs_aiocbp->request_->_cond,
                                      &ovs_aiocbp->request_->_mutex);
                GLOG_DEBUG("Woken Up");
            }
        }
        pthread_mutex_unlock(&ovs_aiocbp->request_->_mutex);
    }
    if (r == ETIMEDOUT)
    {
        r = -1;
        errno = EAGAIN;
        GLOG_DEBUG("TimeOut");
    }
    XXExit();
    return r;
}

int
ovs_aio_cancel(ovs_ctx_t * /*ctx*/,
               struct ovs_aiocb * /*ovs_aiocbp*/)
{
    errno = ENOSYS;
    return -1;
}

ovs_buffer_t*
ovs_allocate(ovs_ctx_t *ctx,
             size_t size)
{
    ovs_buffer_t *buf = (ovs_buffer_t *)malloc(size);
    if (!buf)
    {
        errno = ENOMEM;
    }
    return buf;
}

void*
ovs_buffer_data(ovs_buffer_t *ptr)
{
    if (likely(ptr != NULL))
    {
        return ptr->buf;
    }
    else
    {
        errno = EINVAL;
        return NULL;
    }
}

size_t
ovs_buffer_size(ovs_buffer_t *ptr)
{


    if (likely(ptr != NULL))
    {
        return ptr->size;
    }
    else
    {
        errno = EINVAL;
        return -1;
    }
}

int
ovs_deallocate(ovs_ctx_t *ctx,
               ovs_buffer_t *ptr)
{
    free(ptr);
    return 0;
    
}

ovs_completion_t*
ovs_aio_create_completion(ovs_callback_t complete_cb,
                          void *arg)
{
    ovs_completion_t *completion = NULL;


    if (complete_cb == NULL)
    {
        errno = EINVAL;
        return NULL;
    }
    try
    {
        completion = new ovs_completion_t;
        completion->complete_cb = complete_cb;
        completion->cb_arg = arg;
        completion->_calling = false;
        completion->_on_wait = false;
        completion->_signaled = false;
        completion->_failed = false;
        pthread_cond_init(&completion->_cond, NULL);
        pthread_mutex_init(&completion->_mutex, NULL);
        return completion;
    }
    catch (const std::bad_alloc&)
    {
        errno = ENOMEM;
        return NULL;
    }
}

ssize_t
ovs_aio_return_completion(ovs_completion_t *completion)
{

    if (completion == NULL)
    {
        errno = EINVAL;
        return -1;
    }

    if (not completion->_calling)
    {
        return -1;
    }
    else
    {
        if (not completion->_failed)
        {
            return completion->_rv;
        }
        else
        {
            errno = EIO;
            return -1;
        }
    }
}

int
ovs_aio_wait_completion(ovs_completion_t *completion,
                        const struct timespec *timeout)
{
    int r = 0;

    if (completion == NULL)
    {
        errno = EINVAL;
        return (r = -1);
    }

    if (__sync_bool_compare_and_swap(&completion->_on_wait,
                                     false,
                                     true,
                                     __ATOMIC_RELAXED))
    {
        pthread_mutex_lock(&completion->_mutex);
        while (not completion->_signaled && r == 0)
        {
            if (timeout)
            {
                r = pthread_cond_timedwait(&completion->_cond,
                                           &completion->_mutex,
                                           timeout);
            }
            else
            {
                r = pthread_cond_wait(&completion->_cond,
                                      &completion->_mutex);
            }
        }
        pthread_mutex_unlock(&completion->_mutex);
    }
    if (r == ETIMEDOUT)
    {
        r = -1;
        errno = EAGAIN;
    }
    return r;
}

int
ovs_aio_signal_completion(ovs_completion_t *completion)
{

    if (completion == NULL)
    {
        errno = EINVAL;
        return -1;
    }
    if (not __sync_bool_compare_and_swap(&completion->_on_wait,
                                         false,
                                         true,
                                         __ATOMIC_RELAXED))
    {
        pthread_mutex_lock(&completion->_mutex);
        completion->_signaled = true;
        pthread_cond_signal(&completion->_cond);
        pthread_mutex_unlock(&completion->_mutex);
    }
    return 0;
}

int
ovs_aio_release_completion(ovs_completion_t *completion)
{

    if (completion == NULL)
    {
        errno = EINVAL;
        return -1;
    }
    pthread_mutex_destroy(&completion->_mutex);
    pthread_cond_destroy(&completion->_cond);
    delete completion;
    return 0;
}

int
ovs_aio_readcb(ovs_ctx_t *ctx,
               const std::string& filename,
               struct ovs_aiocb *ovs_aiocbp,
               ovs_completion_t *completion)
{
    return _ovs_submit_aio_request(ctx,
                                   filename,
                                   ovs_aiocbp,
                                   completion,
                                   RequestOp::Read);
}

ssize_t
ovs_read(ovs_ctx_t *ctx,
         const std::string& filename, 
         void *buf,
         size_t nbytes,
         off_t offset)
{
    ssize_t r;
    struct ovs_aiocb aio;
    aio.aio_buf = buf;
    aio.aio_nbytes = nbytes;
    aio.aio_offset = offset;
    if (ctx == NULL)
    {
        errno = EINVAL;
        return (r = -1);
    }

    if ((r = ovs_aio_read(ctx, filename, &aio)) < 0)
    {
        return r;
    }

    if ((r = ovs_aio_suspend(ctx, &aio, NULL)) < 0)
    {
        return r;
    }

    r = ovs_aio_return(ctx, &aio);
    if (ovs_aio_finish(ctx, &aio) < 0)
    {
        r = -1;
    }
    return r;
}
