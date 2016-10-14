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
#include "NetworkXioClient.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#ifdef __GNUC__
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#else
#define likely(x) (x)
#define unlikely(x) (x)
#endif

namespace gobjfs {
namespace xio {

struct client_ctx {
  std::vector<client_ctx_attr_ptr> attr_vec;
  std::vector<std::string> uri_vec;
  gobjfs::xio::NetworkXioClientPtr net_client_;

  ~client_ctx() { net_client_.reset(); }
};


client_ctx_attr_ptr ctx_attr_new() {
  try {
    auto attr = std::make_shared<client_ctx_attr>();
    attr->transport = TransportType::Error;
    attr->port = 0;
    return attr;
  } catch (const std::bad_alloc &) {
    errno = ENOMEM;
  }
  return nullptr;
}

int ctx_attr_set_transport(client_ctx_attr_ptr attr,
                           std::string const &transport,
                           std::string const &host, int port) {
  if (attr == nullptr) {
    errno = EINVAL;
    return -1;
  }

  if ((transport == "tcp") and (false == host.empty())) {
    attr->transport = TransportType::TCP;
    attr->host = host;
    attr->port = port;
    return 0;
  }

  if ((transport == "rdma") and (false == host.empty())) {
    attr->transport = TransportType::RDMA;
    attr->host = host;
    attr->port = port;
    return 0;
  }
  errno = EINVAL;
  return -1;
}

client_ctx_ptr ctx_new(const std::vector<client_ctx_attr_ptr> &attr_vec) {

  client_ctx_ptr ctx = std::make_shared<client_ctx>();
  ctx->attr_vec = attr_vec;

  for (auto& attr: attr_vec) {
    if (attr->transport != TransportType::TCP &&
        attr->transport != TransportType::RDMA) {
      errno = EINVAL;
      ctx = nullptr;
      break;
    }

    std::string uri = "tcp://" + attr->host + ":" + std::to_string(attr->port);
    ctx->uri_vec.push_back(uri);
  }

  return ctx;
}

client_ctx_ptr ctx_new(const client_ctx_attr_ptr attr) {

  if (not attr) {
    errno = EINVAL;
    return nullptr;
  }

  if (attr->transport != TransportType::TCP &&
      attr->transport != TransportType::RDMA) {
    errno = EINVAL;
    return nullptr;
  }

  client_ctx_ptr ctx = nullptr;

  try {
    ctx = std::make_shared<client_ctx>();
    ctx->attr_vec.push_back(attr);
    ctx->net_client_ = nullptr;
  } catch (const std::bad_alloc &) {
    errno = ENOMEM;
    return nullptr;
  }

  switch (attr->transport) {
    case TransportType::TCP: {
      std::string uri = "tcp://" + attr->host + ":" + std::to_string(attr->port);
      ctx->uri_vec.push_back(uri);
      break;
    }
    case TransportType::RDMA: {
      std::string uri = "rdma://" + attr->host + ":" + std::to_string(attr->port);
      ctx->uri_vec.push_back(uri);
      break;
    }
    default: {
      errno = EINVAL;
      ctx = nullptr;
    }
  }
  return ctx;
}

int ctx_init(client_ctx_ptr ctx) {

  int err = 0;

  if (!ctx) {
    errno = EINVAL;
    return -1;
  }

  try {
    ctx->net_client_ =
        std::make_shared<gobjfs::xio::NetworkXioClient>(256);

    for (auto& uri : ctx->uri_vec) { 
      err = ctx->net_client_->connect(uri);
      if (err != 0) {
        break;
      }
    }
  } catch (...) {
    err = -EIO;
  }

  return err;
}

std::string ctx_get_stats(client_ctx_ptr ctx) {
  std::string ret_string;

  if (ctx && ctx->net_client_) {
    ret_string = ctx->net_client_->stats.ToString();
  } else {
    std::ostringstream s;
    s << "ctx=" << ctx.get();
    if (ctx) {
      s << " netclient=" << ctx->net_client_.get();
    }
    ret_string = "invalid ctx or client connection " + s.str();
  }

  return ret_string;
}

bool ctx_is_disconnected(client_ctx_ptr ctx, int32_t uri_slot) {
  if (ctx && ctx->net_client_) {
    return ctx->net_client_->is_disconnected(uri_slot);
  } else {
    return true;
  }
}

static aio_request *create_new_request(RequestOp op, struct giocb *aio,
                                       notifier_sptr cvp, completion *cptr) {
  try {
    aio_request *request = new aio_request;
    request->_op = op;
    request->giocbp = aio;
    request->cptr = cptr;
    /*cnanakos TODO: err handling */
    request->_on_suspend = false;
    request->_canceled = false;
    request->_completed = false;
    request->_signaled = false;
    request->_rv = 0;
    request->_cvp = cvp;
    request->_rtt_nanosec = 0;
    if (aio and op != RequestOp::Noop) {
      aio->request_ = request;
    }
    return request;
  } catch (const std::bad_alloc &) {
    GLOG_ERROR("malloc for aio_request failed");
    return NULL;
  }
}

static int _submit_aio_request(client_ctx_ptr ctx, 
    const std::vector<std::string> &filename_vec,
    const std::vector<giocb*> &giocbp_vec, 
    notifier_sptr &cvp,
    completion *completion, 
    const RequestOp &op,
    int32_t uri_slot) {

  int r = 0;
  gobjfs::xio::NetworkXioClientPtr net_client = ctx->net_client_;

  if (ctx == nullptr || giocbp_vec.size() == 0) {
    errno = EINVAL;
    return -1;
  }

  if (op != RequestOp::Read) {
    errno = EBADF;
    return -1;
  }

  std::vector<void*> request_vec;
  std::vector<void *> bufVec;
  std::vector<uint64_t> sizeVec;
  std::vector<uint64_t> offsetVec;

  for (auto giocbp : giocbp_vec) {

    if ((giocbp->aio_nbytes <= 0 || giocbp->aio_offset < 0)) {
      errno = EINVAL;
      r = -1;
      break;
    }
  
    aio_request *request = create_new_request(op, giocbp, cvp, completion);
    if (request == nullptr) {
      GLOG_ERROR("create_new_request() failed \n");
      errno = ENOMEM;
      r = -1;
      break;
    }

    request_vec.push_back(request);
    bufVec.push_back(giocbp->aio_buf);
    offsetVec.push_back(giocbp->aio_offset);
    sizeVec.push_back(giocbp->aio_nbytes);
  }

  if (r != 0) {
    // TODO clear request_vec
    return r;
  }

  {
    try {
      net_client->send_multi_read_request(filename_vec, bufVec,
                                        sizeVec, offsetVec,
                                        request_vec,
                                        uri_slot);
      for (auto request : request_vec) {
        ((aio_request*)request)->_timer.reset();
      }

    } catch (const std::bad_alloc &) {
      errno = ENOMEM;
      r = -1;
      GLOG_ERROR("xio_send_read_request() failed \n");
    } catch (...) {
      errno = EIO;
      r = -1;
      GLOG_ERROR("xio_send_read_request() failed \n");
    }
  } 

  if (r != 0) {
    GLOG_ERROR(" Remove request send failed with error " << r);
    // TODO clear request_vec
  }
  return r;
}

int aio_error(client_ctx_ptr ctx, giocb *giocbp) {
  int r = 0;
  if (ctx == nullptr || giocbp == nullptr) {
    errno = EINVAL;
    return (r = -1);
  }

  if (giocbp->request_->_canceled) {
    return (r = ECANCELED);
  }

  if (not giocbp->request_->_completed) {
    return (r = EINPROGRESS);
  }

  if (giocbp->request_->_failed) {
    return (r = giocbp->request_->_errno);
  } else {
    return r;
  }
}

ssize_t aio_return(client_ctx_ptr ctx, giocb *giocbp) {
  int r = 0;
  XXEnter();
  if (ctx == nullptr || giocbp == nullptr) {
    GLOG_ERROR("ctx or giocbp NULL");
    r = -EINVAL;
    XXExit();
    return r;
  }

  errno = giocbp->request_->_errno;
  if (not giocbp->request_->_failed) {
    r = giocbp->request_->_rv;
  } else {
    r = GetNegative(errno);
    GLOG_ERROR("giocbp->request_->_failed is true. Error is " << r);
  }
  XXExit();
  return r;
}

int aio_finish(client_ctx_ptr ctx, giocb *giocbp) {
  XXEnter();
  if (ctx == nullptr || giocbp == nullptr) {
    errno = EINVAL;
    GLOG_ERROR("ctx or giocbp NULL ");
    return -1;
  }

  delete giocbp->request_;
  XXExit();
  return 0;
}

int aio_suspendv(client_ctx_ptr ctx, const std::vector<giocb *> &giocbp_vec,
                 const timespec *timeout) {
  int r = 0;
  if (ctx == nullptr || giocbp_vec.size() == 0) {
    errno = EINVAL;
    XXExit();
    return (r = -1);
  }

  bool more_work = false;
  do {

    more_work = false;

    for (auto& elem : giocbp_vec) {
      if (not elem->request_->_completed) {
        more_work = true;
      } else if (elem->request_->_failed) {
        // wait until all requests are complete
        // otherwise proper error codes are not propagated
        errno = elem->request_->_errno;
        r = -1;
      }
    }

    if (more_work) {
      ctx->net_client_->run_loop();
    }

  } while (more_work);

  return r;
}

int aio_suspend(client_ctx_ptr ctx, giocb *giocbp, const timespec *timeout) {
  int r = 0;
  if (ctx == nullptr || giocbp == nullptr) {
    errno = EINVAL;
    return (r = -1);
  }
  bool more_work = false;
  do {

    more_work = false;

    if (not giocbp->request_->_completed) {
      more_work = true;
    } else if (giocbp->request_->_failed) {
      // break out on first error
      // let caller check individual error codes using aio_return
      errno = giocbp->request_->_errno;
      r = -1;
      break;
    }

    if (more_work) {
      ctx->net_client_->run_loop();
    }
  } while (more_work);

  return r;
}

int aio_cancel(client_ctx_ptr /*ctx*/, giocb * /*giocbp*/) {
  errno = ENOSYS;
  return -1;
}

gbuffer *gbuffer_allocate(client_ctx_ptr ctx, size_t size) {
  gbuffer *buf = (gbuffer *)malloc(size);
  if (!buf) {
    errno = ENOMEM;
  }
  return buf;
}

void *gbuffer_data(gbuffer *ptr) {
  if (likely(ptr != nullptr)) {
    return ptr->buf;
  } else {
    errno = EINVAL;
    return nullptr;
  }
}

size_t gbuffer_size(gbuffer *ptr) {

  if (likely(ptr != nullptr)) {
    return ptr->size;
  } else {
    errno = EINVAL;
    return -1;
  }
}

int gbuffer_deallocate(client_ctx_ptr ctx, gbuffer *ptr) {
  free(ptr);
  return 0;
}

completion *aio_create_completion(gcallback complete_cb, void *arg) {
  completion *cptr = nullptr;

  if (complete_cb == nullptr) {
    errno = EINVAL;
    return nullptr;
  }
  try {
    cptr = new completion;
    cptr->complete_cb = complete_cb;
    cptr->cb_arg = arg;
    cptr->_calling = false;
    cptr->_on_wait = false;
    cptr->_signaled = false;
    cptr->_failed = false;
    return cptr;
  } catch (const std::bad_alloc &) {
    errno = ENOMEM;
    return nullptr;
  }
}

ssize_t aio_return_completion(completion *completion) {

  if (completion == nullptr) {
    errno = EINVAL;
    return -1;
  }

  assert(completion->_signaled == true);
  if (not completion->_calling) {
    return -1;
  } else {
    if (not completion->_failed) {
      return completion->_rv;
    } else {
      errno = EIO;
      return -1;
    }
  }
}

int aio_wait_completion(client_ctx_ptr& ctx, completion *completion, const timespec *timeout) {
  int r = 0;

  if (completion == nullptr) {
    errno = EINVAL;
    return (r = -1);
  }

  do {
    // TODO pass timespec here
    ctx->net_client_->run_loop();
  } while (not completion->_signaled);

  return r;
}

int aio_signal_completion(completion *completion) {

  if (completion == nullptr) {
    errno = EINVAL;
    return -1;
  }
  completion->_signaled = true;
  return 0;
}

int aio_release_completion(completion *completion) {

  if (completion == nullptr) {
    errno = EINVAL;
    return -1;
  }
  delete completion;
  return 0;
}

int aio_read(client_ctx_ptr ctx, const std::string &filename, giocb *giocbp, int32_t uri_slot) {

  auto cv = std::make_shared<notifier>();

  std::vector<std::string> filename_vec(1, filename);
  std::vector<giocb*> giocbp_vec(1, giocbp);
  return _submit_aio_request(ctx, filename_vec, giocbp_vec, cv, nullptr,
                             RequestOp::Read, uri_slot);
}

int aio_readv(client_ctx_ptr ctx, const std::vector<std::string> &filename_vec,
              const std::vector<giocb *> &giocbp_vec, int32_t uri_slot) {
  int err = 0;

  if (filename_vec.size() != giocbp_vec.size()) {
    GLOG_ERROR("mismatch between filename vector size="
               << filename_vec.size()
               << " and iocb vector size=" << giocbp_vec.size());
    errno = EINVAL;
    return -1;
  }

  auto cv = std::make_shared<notifier>(giocbp_vec.size());

  err = _submit_aio_request(ctx, filename_vec, giocbp_vec, cv, nullptr,
                               RequestOp::Read, uri_slot);

  return err;
}

int aio_readcb(client_ctx_ptr ctx, const std::string &filename, giocb *giocbp,
               completion *completion, int32_t uri_slot) {
  auto cv = std::make_shared<notifier>();

  std::vector<std::string> filename_vec(1, filename);
  std::vector<giocb*> giocbp_vec(1, giocbp);
  return _submit_aio_request(ctx, filename_vec, giocbp_vec, cv, completion,
                             RequestOp::Read, uri_slot);
}

ssize_t read(client_ctx_ptr ctx, const std::string &filename, void *buf,
             size_t nbytes, off_t offset, int32_t uri_slot) {
  ssize_t r;
  giocb aio;
  aio.aio_buf = buf;
  aio.aio_nbytes = nbytes;
  aio.aio_offset = offset;
  if (ctx == nullptr) {
    errno = EINVAL;
    return (r = -1);
  }

  if ((r = aio_read(ctx, filename, &aio, uri_slot)) < 0) {
    return r;
  }

  r = aio_suspend(ctx, &aio, nullptr);

  if (r == 0) {
    r = aio_return(ctx, &aio);
  }

  if (aio_finish(ctx, &aio) < 0) {
    r = -1;
  }
  return r;
}
}
}
