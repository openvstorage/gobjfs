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

#include "PortalThreadData.h"
#include "NetworkXioServer.h"
#include "NetworkXioIOHandler.h"
#include <util/os_utils.h>
#include <gobjfs_log.h>
#include <IOExecutor.h>

namespace gobjfs {
namespace xio {

template <class T>
static void static_evfd_stop_loop(int fd, int events, void *data) {
  XXEnter();
  T *obj = reinterpret_cast<T *>(data);
  if (obj == NULL) {
    XXExit();
    return;
  }
  obj->evfd_stop_loop(fd, events, data);
  XXExit();
}

/**
 * Only called for Portal
 */
template <class T>
static int static_on_request(xio_session *session, xio_msg *req,
                             int last_in_rxq, void *cb_user_context) {
  T *obj = reinterpret_cast<T *>(cb_user_context);
  if (obj == NULL) {
    return -1;
  }
  return obj->server_->on_request(session, req, last_in_rxq,
                                     cb_user_context);
}

/**
 * Only called for Portal
 */
template <class T>
static int static_on_ow_msg_send_complete(xio_session *session, xio_msg *msg,
                                       void *cb_user_context) {
  XXEnter();
  T *obj = reinterpret_cast<T *>(cb_user_context);
  if (obj == NULL) {
    XXExit();
    return -1;
  }
  XXExit();
  return obj->server_->on_msg_send_complete(session, msg, cb_user_context);
}

void PortalThreadData::evfd_stop_loop(int /*fd*/, int /*events*/, void * /*data*/) {
  evfd_.readfd();
  xio_context_stop_loop(ctx_);
}

void PortalThreadData::stop_loop() {
  stopping = true;
  evfd_.writefd();
}

void PortalThreadData::changeNumConnections(int change) {
  numConnections_ += change;
}

void PortalThreadData::portal_func() {

  gobjfs::os::BindThreadToCore(coreId_);
  
  // if context_params is null, max 100 connections per ctx
  // see max_conns_per_ctx in accelio code
  ctx_ = xio_context_create(NULL, 0, coreId_);
  
  mpool_ = server_->xio_mpool.get();

  ioh_ = new NetworkXioIOHandler(this);

  if (xio_context_add_ev_handler(ctx_, evfd_, XIO_POLLIN,
                                 static_evfd_stop_loop<PortalThreadData>,
                                 this)) {
    throw FailedRegisterEventHandler("failed to register event handler");
  }

  xio_session_ops portal_ops;
  portal_ops.on_session_event = NULL;
  portal_ops.on_new_session = NULL;
  portal_ops.on_ow_msg_send_complete = static_on_ow_msg_send_complete<PortalThreadData>;
  portal_ops.on_msg = static_on_request<PortalThreadData>;

  xio_server_ = xio_bind(ctx_, &portal_ops, uri_.c_str(), NULL, 0, this);

  GLOG_INFO("started portal ptr=" << (void*)this
      << ",coreId=" << coreId_ 
      << ",ioh=" << (void*)ioh_ 
      << ",evfd=" << evfd_ 
      << ",uri=" << uri_ 
      << ",thread=" << gettid());

  //xio_context_run_loop(ctx_, XIO_INFINITE);
  int numIdleLoops = 0;
  while (not stopping) {
    int timeout_ms = XIO_INFINITE;
    const size_t pendingBefore = ioh_->numPendingRequests();
    if (pendingBefore) {
      // if workQueue is small, do quick check to see if there are more requests
      timeout_ms = 0;
      numIdleLoops ++;
    }
    int numEvents = xio_context_run_loop(ctx_, timeout_ms);
    if (numIdleLoops == 1) {
      const size_t pendingAfter = ioh_->numPendingRequests();
      // if no new req received from network 
      // then we must manually drain the queue
      ioh_->drainQueue();
      numIdleLoops = 0;
    }
    (void) numEvents;
  }

  delete ioh_;

  xio_context_del_ev_handler(ctx_, evfd_);

  xio_unbind(xio_server_);

  xio_context_destroy(ctx_);

  GLOG_INFO("stopped portal ptr=" << (void*)this
      << ",coreId=" << coreId_ 
      << ",uri=" << uri_ 
      << ",thread=" << gettid());
}

PortalThreadData::~PortalThreadData() {
  // TODO free others
}

}
}
