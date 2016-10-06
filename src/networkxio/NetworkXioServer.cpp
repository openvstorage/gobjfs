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

#include <libxio.h>
#include <future>
#include <mutex>
#include <sys/epoll.h> // epoll_create1
#include <boost/thread/lock_guard.hpp>

#include <networkxio/gobjfs_client_common.h>
#include <networkxio/gobjfs_config.h>
#include <gobjfs_client.h>
#include <util/os_utils.h>

#include "NetworkXioServer.h"
#include "NetworkXioProtocol.h"
#include "NetworkXioRequest.h"
#include "gobjfs_getenv.h"

using gobjfs::os::DirectIOSize;

namespace gobjfs {
namespace xio {

static constexpr int XIO_COMPLETION_DEFAULT_MAX_EVENTS = 100;

static std::string geturi(xio_session *s) {
	xio_session_attr attr;
	int ret = xio_query_session(s, &attr, XIO_SESSION_ATTR_URI);
	if (ret == 0) return attr.uri;
	return "";
}

// TODO cleanup duplicate of function existing in NetworkXioIOHandler
static inline void pack_msg(NetworkXioRequest *req) {
  NetworkXioMsg o_msg(req->op);
  o_msg.retval(req->retval);
  o_msg.errval(req->errval);
  o_msg.opaque(req->opaque);
  req->s_msg = o_msg.pack_msg();
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
static int static_on_msg_send_complete(xio_session *session, xio_msg *msg,
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

/**
 * Only called for Server
 */
template <class T>
static int static_on_session_event(xio_session *session,
                                   xio_session_event_data *event_data,
                                   void *cb_user_context) {
  void* tdata = nullptr;

  // cb_user_context = user_context associated with session
  // event_data->conn_user_context = user context associated
  // with the connection
  // if they are equal, then its an event on main thread
  // else its an event for portal

  if (event_data->conn_user_context == cb_user_context) {
    // this is a global server event
  } else {
    // this is a portal event 
    // for new connection, it will be a ptr to PortalThreadData
    // for connection being destroyed, it will be ptr to NetworkClientData
    // since we modify the connection user ctx
    tdata = event_data->conn_user_context;
  }

  xio_connection_attr conn_attr;
  int ret = xio_query_connection(event_data->conn, 
    &conn_attr,
    XIO_CONNECTION_ATTR_PEER_ADDR);

  const char* ipAddr = nullptr;
  int port = -1;
  if (ret == 0) {
    sockaddr_in *sa = (sockaddr_in*)&conn_attr.peer_addr;
    ipAddr = inet_ntoa(sa->sin_addr);
    port = sa->sin_port;
  }

  GLOG_INFO("got session event=" << xio_session_event_str(event_data->event)
      << ",reason=" << xio_strerror(event_data->reason)
      << ",is_portal=" << (tdata != nullptr)
      << ",thread=" << gettid() 
      << ",addr=" << (ipAddr ? ipAddr : "null")
      << ",port=" << port
      << ",uri=" << geturi(session));

  // TODO
  T *obj = reinterpret_cast<T *>(cb_user_context);
  if (obj == NULL) {
    return -1;
  }
  return obj->on_session_event(session, event_data, tdata);
}

/**
 * Only called for Server
 */
template <class T>
static int static_on_new_session(xio_session *session, xio_new_session_req *req,
                                 void *cb_user_context) {
  T *obj = reinterpret_cast<T *>(cb_user_context);
  if (obj == NULL) {
    XXExit();
    return -1;
  }
  return obj->on_new_session(session, req);
}



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
  portal_ops.on_msg_send_complete = static_on_msg_send_complete<PortalThreadData>;
  portal_ops.on_msg = static_on_request<PortalThreadData>;

  xio_server_ = xio_bind(ctx_, &portal_ops, uri_.c_str(), NULL, 0, this);

  GLOG_INFO("started portal coreId=" << coreId_ << " uri=" << uri_ << " with thread=" << gettid());

  int numIdleLoops = 0;
  while (not stopping) {
    int timeout_ms = XIO_INFINITE;
    if (ioh_->alreadyInvoked()) {
      // if workQueue is small, just do a quick check if there are more requests
      timeout_ms = 0;
      numIdleLoops ++;
    }
    int numEvents = xio_context_run_loop(ctx_, timeout_ms);
    if ((timeout_ms == 0) && (numIdleLoops == 2)) {
      // if no req received, then handler was not called
      // so we must manually drain the queue
      ioh_->drainQueue();
      numIdleLoops = 0;
    }
    (void) numEvents;
  }

  xio_context_del_ev_handler(ctx_, evfd_);

  xio_unbind(xio_server_);

  xio_context_destroy(ctx_);
}

NetworkXioServer::NetworkXioServer(const std::string &transport,
                                    const std::string &ipaddr,
                                    int port,
                                   int32_t numCoresForIO,
                                   int32_t queueDepthForIO,
                                   FileTranslatorFunc fileTranslatorFunc,
                                   bool newInstance, size_t snd_rcv_queue_depth)
    : transport_(transport),
      ipaddr_(ipaddr),
      port_(port), 
      numCoresForIO_(numCoresForIO),
      queueDepthForIO_(queueDepthForIO),
      fileTranslatorFunc_(fileTranslatorFunc), newInstance_(newInstance),
      stopping(false), stopped(false), 
      queue_depth(snd_rcv_queue_depth) {}


void NetworkXioServer::destroy_ctx_shutdown(xio_context *ctx) {
  xio_context_destroy(ctx);
  xio_shutdown();
}

NetworkXioServer::~NetworkXioServer() { shutdown(); }

// TODO same for Portal
void NetworkXioServer::evfd_stop_loop(int /*fd*/, int /*events*/,
                                      void * /*data*/) {
  evfd.readfd();
  xio_context_stop_loop(ctx.get());
}


void NetworkXioServer::run(std::promise<void> &promise) {
  int xopt = 2;

  XXEnter();

  xio_init();

  xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_MAX_IN_IOVLEN, &xopt,
              sizeof(xopt));

  xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_MAX_OUT_IOVLEN, &xopt,
              sizeof(xopt));

  xopt = 0;
  xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_ENABLE_FLOW_CONTROL,
              &xopt, sizeof(xopt));

  xopt = queue_depth;
  xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_SND_QUEUE_DEPTH_MSGS,
              &xopt, sizeof(xopt));

  xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_RCV_QUEUE_DEPTH_MSGS,
              &xopt, sizeof(xopt));

  xopt = 1;
  xio_set_opt(NULL, XIO_OPTLEVEL_TCP, XIO_OPTNAME_TCP_NO_DELAY,
              &xopt, sizeof(xopt));

  const int polling_time_usec = getenv_with_default("GOBJFS_POLLING_TIME_USEC", POLLING_TIME_USEC_DEFAULT);

  ctx = std::shared_ptr<xio_context>(
      xio_context_create(NULL, polling_time_usec, -1),
      destroy_ctx_shutdown);

  if (ctx == nullptr) {
    throw FailedCreateXioContext("failed to create XIO context");
  }

  std::string uri = transport_ + "://" + ipaddr_ + ":" + std::to_string(port_);

  xio_session_ops server_ops;
  server_ops.on_session_event = static_on_session_event<NetworkXioServer>;
  server_ops.on_new_session = static_on_new_session<NetworkXioServer>;
  server_ops.on_msg_send_complete = NULL;
  server_ops.on_msg = NULL;
  server_ops.assign_data_in_buf = NULL;
  server_ops.on_msg_error = NULL;

  GLOG_INFO("bind XIO server to '" << uri << "'");
  server = std::shared_ptr<xio_server>(
      xio_bind(ctx.get(), &server_ops, uri.c_str(), NULL, 0, this), xio_unbind);
  if (server == nullptr) {
    throw FailedBindXioServer("failed to bind XIO server");
  }

  xio_mpool = std::shared_ptr<xio_mempool>(
      xio_mempool_create(-1, XIO_MEMPOOL_FLAG_REG_MR), xio_mempool_destroy);
  if (xio_mpool == nullptr) {
    GLOG_FATAL("failed to create XIO memory pool");
    throw FailedCreateXioMempool("failed to create XIO memory pool");
  }
  (void)xio_mempool_add_slab(xio_mpool.get(), 4096, 0, queue_depth, 32,
                             DirectIOSize);
  (void)xio_mempool_add_slab(xio_mpool.get(), 32768, 0, queue_depth, 32,
                             DirectIOSize);
  (void)xio_mempool_add_slab(xio_mpool.get(), 65536, 0, queue_depth, 32,
                             DirectIOSize);
  (void)xio_mempool_add_slab(xio_mpool.get(), 131072, 0, 256, 32, DirectIOSize);
  (void)xio_mempool_add_slab(xio_mpool.get(), 1048576, 0, 32, 4, DirectIOSize);


  // create multiple ports for load-balancing
  // tie a thread to each port
  // then xio_accept() will decide which port+thread to use for a new connection
  for (int i = 0; i < numCoresForIO_; i++) {
    std::string uri = transport_ + "://" + ipaddr_ + ":" + std::to_string(port_ + i + 1);
    auto newpt = new PortalThreadData(this, uri, i);
    newpt->thread_ = std::thread(std::bind(&PortalThreadData::portal_func, newpt));
    ptVec_.push_back(newpt);
  }

  promise.set_value();

  while (not stopping) {
    int ret = xio_context_run_loop(ctx.get(), XIO_INFINITE);
    assert(ret == 0);
  }

  for (auto elem : ptVec_) {
    elem->thread_.join();
  }

  server.reset();
  ctx.reset();
  xio_mpool.reset();

  std::lock_guard<std::mutex> lock_(mutex_);
  stopped = true;
  cv_.notify_one();

  XXExit();
}

int
NetworkXioServer::create_session_connection(xio_session *session,
                                            xio_session_event_data *evdata,
                                            PortalThreadData* pt) {
  NetworkXioClientData* cd = nullptr;

  try {

    cd = new NetworkXioClientData(pt, session, evdata->conn);
    cd->server_ = pt->server_;
    // for established connection, set user_ctx to be NetworkXioClientData
    // instead of PortalThreadData
    xio_connection_attr xconattr;
    xconattr.user_context = cd;
    (void)xio_modify_connection(evdata->conn, &xconattr,
        XIO_CONNECTION_ATTR_USER_CTX);

    pt->numConnections_ ++;
    GLOG_INFO("portal=" << pt->coreId_ 
        << " thread=" << gettid()
        << " now serving " << pt->numConnections_ << " connections(up 1)");

  } catch (...) {

    GLOG_ERROR("cannot create client data ");
    return -1;

  }

  return 0;
}

int NetworkXioServer::on_new_session(xio_session *session,
                                     xio_new_session_req *req) {

  const char* portal_array[ptVec_.size()];

  // tell xio_accept which portals are available
  for (int i = 0; i < ptVec_.size(); i++) {
    portal_array[i] = ptVec_[i]->uri_.c_str();
  }

  if (xio_accept(session, portal_array, ptVec_.size(), NULL, 0) < 0) {
    GLOG_ERROR(
        "cannot accept new session, error: " << xio_strerror(xio_errno()));
  }

  sockaddr_in *sa = (sockaddr_in*)&req->src_addr;
  const char *ipAddr = inet_ntoa(sa->sin_addr);
  std::string uriStr(req->uri, req->uri_len);
  GLOG_INFO("Got a new session request for uri=" << uriStr.c_str()
      << " thread=" << gettid()
      << " and addr=" << ipAddr 
      << " port=" << sa->sin_port);

  return 0;
}

int NetworkXioServer::on_session_event(xio_session *session,
                                       xio_session_event_data *event_data,
                                       void* tdata) {

  switch (event_data->event) {

  case XIO_SESSION_NEW_CONNECTION_EVENT:
    if (tdata) {
      // signals a new connection for portal
      create_session_connection(session, event_data, (PortalThreadData*)tdata);
    } else {
      // ignore
    }
    break;
  case XIO_SESSION_CONNECTION_ERROR_EVENT:

  case XIO_SESSION_CONNECTION_TEARDOWN_EVENT:
    // signals loss of a connection within existing session
    if (tdata) {
      NetworkXioClientData* cd = (NetworkXioClientData*)tdata;
      cd->conn_state = 2;
      if (cd->ncd_refcnt != 0) {
        GLOG_INFO("deferring free since clientdata=" << (void*)cd 
            << " has unsent replies=" << cd->ncd_refcnt
            << " msg list size=" << cd->ncd_done_reqs.size());
      } else {
        assert(cd->ncd_conn == event_data->conn);
        xio_connection_destroy(cd->ncd_conn);
    	cd->pt_->numConnections_ --;
    	GLOG_INFO("portal=" << cd->pt_->coreId_ << " now serving " << cd->pt_->numConnections_ << " connections(down 1)");
        delete cd;
      }
    }
    break;
  case XIO_SESSION_TEARDOWN_EVENT:
    // signals loss of session
    // cannot directly call xio_context_stop_loop() on portal ctx
    // since the portal threads own that ctx and are accessing it
    // while this function is being run in the main thread
    // so we write to the evfd instead
    for (auto elem : ptVec_) {
      elem->stop_loop();
    }
    if (ctx) {
      xio_context_stop_loop(ctx.get());
    }
    xio_session_destroy(session);
    break;
  default:
    break;
  };
  XXExit();
  return 0;
}

void NetworkXioServer::deallocate_request(NetworkXioRequest *req) {

  if ((req->op == NetworkXioMsgOpcode::ReadRsp) && req->data) {
    if (req->from_pool) {
      xio_mempool_free(&req->reg_mem);
    } else {
      xio_mem_free(&req->reg_mem);
    }
  }

  delete req;
}

int NetworkXioServer::on_msg_send_complete(xio_session *session
                                               ATTRIBUTE_UNUSED,
                                           xio_msg *msg ATTRIBUTE_UNUSED,
                                           void *cb_user_ctx) {
  NetworkXioClientData *cd =
      static_cast<NetworkXioClientData *>(cb_user_ctx);
  NetworkXioRequest *req = cd->ncd_done_reqs.front();
  cd->ncd_done_reqs.pop_front();
  deallocate_request(req);
  cd->ncd_refcnt --;
  
  if ((cd->conn_state == 2) && (cd->ncd_refcnt == 0)) {
    xio_connection_destroy(cd->ncd_conn);
    GLOG_INFO("closing deferred connection" << cd);
    cd->pt_->numConnections_ ++;
    GLOG_INFO("portal=" << cd->pt_->coreId_ 
        << " thread=" << gettid()
        << " now serving " << cd->pt_->numConnections_ << " connections(down 1)");
    delete cd;
  }
  return 0;
}

void NetworkXioServer::send_reply(NetworkXioRequest *req) {
  XXEnter();
  xio_msg *xio_req = req->xio_req;

  memset(&req->xio_reply, 0, sizeof(xio_msg));

  vmsg_sglist_set_nents(&req->xio_req->in, 0);
  xio_req->in.header.iov_base = NULL;
  xio_req->in.header.iov_len = 0;
  req->xio_reply.request = xio_req;

  req->xio_reply.out.header.iov_base =
      const_cast<void *>(reinterpret_cast<const void *>(req->s_msg.c_str()));
  req->xio_reply.out.header.iov_len = req->s_msg.length();
  if ((req->op == NetworkXioMsgOpcode::ReadRsp) && req->data) {
    vmsg_sglist_set_nents(&req->xio_reply.out, 1);
    req->xio_reply.out.sgl_type = XIO_SGL_TYPE_IOV;
    req->xio_reply.out.data_iov.max_nents = XIO_IOVLEN;
    req->xio_reply.out.data_iov.sglist[0].iov_base = req->data;
    req->xio_reply.out.data_iov.sglist[0].iov_len = req->data_len;
    req->xio_reply.out.data_iov.sglist[0].mr = req->reg_mem.mr;
  }
  req->xio_reply.flags = XIO_MSG_FLAG_IMM_SEND_COMP;

  int ret = xio_send_response(&req->xio_reply);
  if (ret != 0) {
    GLOG_ERROR("failed to send reply: " << xio_strerror(xio_errno()));
    deallocate_request(req);
  } else {
    req->pClientData->ncd_done_reqs.push_back(req);
  }
  XXExit();
}

int NetworkXioServer::on_request(xio_session *session ATTRIBUTE_UNUSED,
                                 xio_msg *xio_req,
                                 int last_in_rxq ATTRIBUTE_UNUSED,
                                 void *cb_user_ctx) {

  try {
    auto clientData = static_cast<NetworkXioClientData *>(cb_user_ctx);
    NetworkXioRequest *req = new NetworkXioRequest(xio_req, clientData);
    req->from_pool = true;
    clientData->ncd_refcnt ++;
    clientData->pt_->ioh_->handle_request(req);
  } catch (const std::bad_alloc &) {
    int ret = xio_cancel(xio_req, XIO_E_MSG_CANCELED);
    GLOG_ERROR("failed to allocate request, cancelling XIO request: " << ret);
  }
  return 0;
}

void NetworkXioServer::shutdown() {
  XXEnter();
  if (not stopped) {
    stopping = true;
    // stop all portal loops 
    for (auto elem : ptVec_) {
      elem->stopping = true;
      elem->stop_loop();
    }
    xio_context_stop_loop(ctx.get());
    {
      std::unique_lock<std::mutex> lock_(mutex_);
      cv_.wait(lock_, [&] { return stopped == true; });
    }
  }
  XXExit();
}
}
} // namespace
