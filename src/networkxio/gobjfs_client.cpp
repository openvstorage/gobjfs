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

#include <cerrno>
#include <limits.h>
#include <map>

#include <networkxio/gobjfs_client_common.h>
#include <gobjfs_client.h>
#include <networkxio/NetworkXioCommon.h>
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

ovs_ctx_attr_ptr
ovs_ctx_attr_new()
{
    try
    {
        auto attr = std::make_shared<ovs_context_attr_t>();
        attr->transport = TransportType::Error;
        attr->port = 0;
        return attr;
    }
    catch (const std::bad_alloc&)
    {
        errno = ENOMEM;
    }
    return nullptr;
}

int
ovs_ctx_attr_set_transport(ovs_ctx_attr_ptr attr,
                           const char *transport,
                           const char *host,
                           int port)
{
    if (attr == nullptr)
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

ovs_ctx_ptr
ovs_ctx_new(const ovs_ctx_attr_ptr attr)
{
    ovs_ctx_ptr ctx = nullptr;

    if(not attr)
    {
        errno = EINVAL;
        return nullptr;
    }

    if (attr->transport != TransportType::TCP &&
        attr->transport != TransportType::RDMA)
    {
        errno = EINVAL;
        return nullptr;
    }

    try
    {
        ctx = std::make_shared<ovs_context_t>();
        ctx->transport = attr->transport;
        ctx->host = attr->host;
        ctx->port = attr->port;
        ctx->net_client_ = nullptr;
    }
    catch (const std::bad_alloc&)
    {
        errno = ENOMEM;
        return nullptr;
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
        return nullptr;
    }
    return ctx;
}

int
ovs_ctx_init(ovs_ctx_ptr ctx)
{
    int err = 0;    
    XXEnter();

    if (ctx->transport == TransportType::RDMA ||
             ctx->transport == TransportType::TCP)
    {
        try
        {
            ctx->net_client_ =
                std::make_shared<gobjfs::xio::NetworkXioClient>(ctx->uri, 256);
        }
        catch (...)
        {
            XXExit();
            err = -EIO;
            XXDone();
        }
        err = ovs_xio_open_device(ctx);
        if (err < 0) {
            GLOG_ERROR("ovs_xio_open_device failed with error " << err);
            XXDone();
        }
    }
done:
    XXExit();
    return err;
}

static int
_ovs_submit_aio_request(ovs_ctx_ptr ctx,
                        const std::string& filename,
                        struct ovs_aiocb *ovs_aiocbp,
                        notifier_sptr& cvp,
                        ovs_completion_t *completion,
                        const RequestOp& op)
{
    XXEnter();
    int r = 0, accmode;
    gobjfs::xio::NetworkXioClientPtr net_client = ctx->net_client_;


    if (ctx == nullptr || ovs_aiocbp == nullptr)
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

    switch (op)
    {
    case RequestOp::Read:
        break;
    default:
        errno = EBADF;
        XXExit();
        return -1;
    }

    ovs_aio_request *request = create_new_request(op, 
                                                  ovs_aiocbp, 
                                                  cvp, 
                                                  completion);
    if (request == nullptr)
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
ovs_aio_error(ovs_ctx_ptr ctx,
              struct ovs_aiocb *ovs_aiocbp)
{
    int r = 0;
    if (ctx == nullptr || ovs_aiocbp == nullptr)
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
ovs_aio_return(ovs_ctx_ptr ctx,
               struct ovs_aiocb *ovs_aiocbp)
{
    int r = 0;
    XXEnter();
    if (ctx == nullptr || ovs_aiocbp == nullptr)
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
ovs_aio_finish(ovs_ctx_ptr ctx,
               struct ovs_aiocb *ovs_aiocbp)
{
    XXEnter();
    if (ctx == nullptr || ovs_aiocbp == nullptr)
    {
        errno = EINVAL;
        GLOG_ERROR("ctx or ovs_aiocbp NULL ");
        return -1;
    }

    delete ovs_aiocbp->request_;
    XXExit();
    return 0;
}

int
ovs_aio_suspendv(ovs_ctx_ptr ctx,
                const std::vector<ovs_aiocb*> &ovs_aiocbp_vec,
                const struct timespec *timeout)
{
    XXEnter();
    int r = 0;
    if (ctx == nullptr || ovs_aiocbp_vec.size() == 0)
    {
        errno = EINVAL;
        XXExit();
        return (r = -1);
    }

    for (auto elem : ovs_aiocbp_vec) {
      __sync_bool_compare_and_swap(&elem->request_->_on_suspend,
                                     false,
                                     true,
                                     __ATOMIC_RELAXED);
    }

    auto cvp = ovs_aiocbp_vec[0]->request_->_cvp;

    {
      if (timeout)
      {
          // TODO add func
          cvp->wait_for(timeout);
      }
      else
      {
          cvp->wait();
      }
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
ovs_aio_suspend(ovs_ctx_ptr ctx,
                ovs_aiocb *ovs_aiocbp,
                const struct timespec *timeout)
{
    XXEnter();
    int r = 0;
    if (ctx == nullptr || ovs_aiocbp == nullptr)
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
      if (timeout)
      {
          auto func = [&] () { return ovs_aiocbp->request_->_signaled; };
          // TODO add func
          ovs_aiocbp->request_->_cvp->wait_for(timeout);
      }
      else
      {
          ovs_aiocbp->request_->_cvp->wait();
      }
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
ovs_aio_cancel(ovs_ctx_ptr  /*ctx*/,
               struct ovs_aiocb * /*ovs_aiocbp*/)
{
    errno = ENOSYS;
    return -1;
}

ovs_buffer_t*
ovs_allocate(ovs_ctx_ptr ctx,
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
    if (likely(ptr != nullptr))
    {
        return ptr->buf;
    }
    else
    {
        errno = EINVAL;
        return nullptr;
    }
}

size_t
ovs_buffer_size(ovs_buffer_t *ptr)
{


    if (likely(ptr != nullptr))
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
ovs_deallocate(ovs_ctx_ptr ctx,
               ovs_buffer_t *ptr)
{
    free(ptr);
    return 0;
    
}

ovs_completion_t*
ovs_aio_create_completion(ovs_callback_t complete_cb,
                          void *arg)
{
    ovs_completion_t *completion = nullptr;


    if (complete_cb == nullptr)
    {
        errno = EINVAL;
        return nullptr;
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
        return completion;
    }
    catch (const std::bad_alloc&)
    {
        errno = ENOMEM;
        return nullptr;
    }
}

ssize_t
ovs_aio_return_completion(ovs_completion_t *completion)
{

    if (completion == nullptr)
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

    if (completion == nullptr)
    {
        errno = EINVAL;
        return (r = -1);
    }

    if (__sync_bool_compare_and_swap(&completion->_on_wait,
                                     false,
                                     true,
                                     __ATOMIC_RELAXED))
    {
        std::unique_lock<std::mutex> l_(completion->_mutex);

        auto func = [&] () { return completion->_signaled; };

        {
            if (timeout)
            {
                completion->_cond.wait_for(
                  l_, 
                  std::chrono::nanoseconds(
                    ((uint64_t)timeout->tv_sec * 1000000000) + 
                      timeout->tv_nsec), 
                  func);
            }
            else
            {
                completion->_cond.wait(l_);
            }
        }
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

    if (completion == nullptr)
    {
        errno = EINVAL;
        return -1;
    }
    if (not __sync_bool_compare_and_swap(&completion->_on_wait,
                                         false,
                                         true,
                                         __ATOMIC_RELAXED))
    {
        std::unique_lock<std::mutex> l_(completion->_mutex);
        completion->_signaled = true;
        completion->_cond.notify_all();
    }
    return 0;
}

int
ovs_aio_release_completion(ovs_completion_t *completion)
{

    if (completion == nullptr)
    {
        errno = EINVAL;
        return -1;
    }
    delete completion;
    return 0;
}

int
ovs_aio_read(ovs_ctx_ptr ctx,
               const std::string& filename,
               struct ovs_aiocb *ovs_aiocbp)
{
  auto cv = std::make_shared<notifier>();

  return _ovs_submit_aio_request(ctx,
                                   filename,
                                   ovs_aiocbp,
                                   cv,
                                   nullptr,
                                   RequestOp::Read);
}

int
ovs_aio_readv(ovs_ctx_ptr ctx,
               const std::vector<std::string>& filename_vec,
               const std::vector<ovs_aiocb*> &ovs_aiocbp_vec)
{
  int err = 0;

  if (filename_vec.size() != ovs_aiocbp_vec.size()) 
  {
    GLOG_ERROR("mismatch between filename vector size=" << filename_vec.size()
      << " and iocb vector size=" << ovs_aiocbp_vec.size());
    errno = EINVAL;
    return -1;
  }

  auto cv = std::make_shared<notifier>(ovs_aiocbp_vec.size());

  size_t idx = 0;
  for (auto elem : ovs_aiocbp_vec) {
    err |= _ovs_submit_aio_request(ctx,
                                   filename_vec[idx++],
                                   elem,
                                   cv,
                                   nullptr,
                                   RequestOp::Read);
  }

  return err;
}

int
ovs_aio_readcb(ovs_ctx_ptr ctx,
               const std::string& filename,
               struct ovs_aiocb *ovs_aiocbp,
               ovs_completion_t *completion)
{
  auto cv = std::make_shared<notifier>();

  return _ovs_submit_aio_request(ctx,
                                   filename,
                                   ovs_aiocbp,
                                   cv,
                                   completion,
                                   RequestOp::Read);
}

ssize_t
ovs_read(ovs_ctx_ptr ctx,
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
    if (ctx == nullptr)
    {
        errno = EINVAL;
        return (r = -1);
    }

    if ((r = ovs_aio_read(ctx, filename, &aio)) < 0)
    {
        return r;
    }

    if ((r = ovs_aio_suspend(ctx, &aio, nullptr)) < 0)
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
