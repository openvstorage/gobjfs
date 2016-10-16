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

#include <util/os_utils.h>
#include <util/lang_utils.h>

#include <networkxio/gobjfs_client_common.h>
#include <networkxio/gobjfs_config.h>
#include <gobjfs_client.h>

#include "NetworkXioServer.h"
#include "NetworkXioProtocol.h"
#include "NetworkXioRequest.h"
#include "NetworkXioCommon.h"
#include "PortalThreadData.h"
#include "gobjfs_getenv.h"

using gobjfs::os::DirectIOSize;

namespace gobjfs {
namespace xio {


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
    // since we modify the connection user ctx using xio_modify_connection()
    tdata = event_data->conn_user_context;
  }

  std::string peerAddr;
  int peerPort = -1;
  std::string localAddr;
  int localPort = -1;
  getAddressAndPort(event_data->conn, localAddr, localPort, peerAddr, peerPort);

  GLOG_INFO("got session event=" << xio_session_event_str(event_data->event)
      << ",reason=" << xio_strerror(event_data->reason)
      << ",conn=" << event_data->conn
      << ",is_portal=" << (tdata != nullptr)
      << ",tdata_ptr=" << (void*)tdata
      << ",cb_user_ctx=" << (void*)cb_user_context
      << ",thread=" << gettid() 
      << ",peer_addr=" << peerAddr.c_str()
      << ",peer_port=" << peerPort
      << ",local_addr=" << localAddr.c_str()
      << ",local_port=" << localPort
      << ",uri=" << getURI(session));

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

NetworkXioServer::NetworkXioServer(const std::string &transport,
                                    const std::string &ipaddr,
                                    int port,
                                   int32_t startCoreForIO,
                                   int32_t numCoresForIO,
                                   int32_t queueDepthForIO,
                                   FileTranslatorFunc fileTranslatorFunc,
                                   bool newInstance, size_t snd_rcv_queue_depth)
    : transport_(transport),
      ipaddr_(ipaddr),
      port_(port), 
	  startCoreForIO_(startCoreForIO),
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

void NetworkXioServer::evfd_stop_loop(int /*fd*/, int /*events*/,
                                      void * /*data*/) {
  evfd.readfd();
  xio_context_stop_loop(ctx.get());
}


void NetworkXioServer::run(std::promise<void> &promise) {

  XXEnter();

  xio_init();

  int ret = 0;
  int xopt = MAX_AIO_BATCH_SIZE;
  ret = xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_MAX_IN_IOVLEN, &xopt,
              sizeof(xopt));

  ret = xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_MAX_OUT_IOVLEN, &xopt,
              sizeof(xopt));

  xopt = 0;
  ret = xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_ENABLE_FLOW_CONTROL,
              &xopt, sizeof(xopt));

  xopt = queue_depth;
  ret = xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_SND_QUEUE_DEPTH_MSGS,
              &xopt, sizeof(xopt));

  ret = xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_RCV_QUEUE_DEPTH_MSGS,
              &xopt, sizeof(xopt));

  xopt = 1;
  ret = xio_set_opt(NULL, XIO_OPTLEVEL_TCP, XIO_OPTNAME_TCP_NO_DELAY,
              &xopt, sizeof(xopt));

  xopt = MAX_INLINE_HEADER_OR_DATA; 
  ret = xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_MAX_INLINE_XIO_HEADER,
              &xopt, sizeof(xopt));

  ret = xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_MAX_INLINE_XIO_DATA,
              &xopt, sizeof(xopt));
  // TODO print warning if ret non-zero

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
    auto newpt = gobjfs::make_unique<PortalThreadData>(this, uri, startCoreForIO_ + i + 1);
    newpt->thread_ = std::thread(std::bind(&PortalThreadData::portal_func, newpt.get()));
    ptVec_.push_back(std::move(newpt));
  }

  promise.set_value();

  // bind this thread to core different than the portal threads
  gobjfs::os::BindThreadToCore(startCoreForIO_);

  while (not stopping) {
    int ret = xio_context_run_loop(ctx.get(), XIO_INFINITE);
    assert(ret == 0);
  }

  for (auto& pt : ptVec_) {
    pt->thread_.join();
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

    pt->changeNumConnections(1);

    GLOG_INFO("portal=" << pt->coreId_ 
        << ",thread=" << gettid()
        << ",new clientData=" << (void*)cd
        << " now serving " << pt->numConnections() << " connections(up 1)");

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
  for (size_t i = 0; i < ptVec_.size(); i++) {
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
      << ",thread=" << gettid()
      << ",addr=" << ipAddr
      << ",port=" << sa->sin_port
      );

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
      // new conn received on main session - do nothing
      // since we are using portals, this connection will be closed 
      // and then a event for new portal connection will be received
    }
    break;
  case XIO_SESSION_CONNECTION_ERROR_EVENT:
    xio_disconnect(event_data->conn);
    GLOG_INFO("disconnecting conn=" << event_data->conn << " thread=" << gettid());
    break;

  case XIO_SESSION_CONNECTION_TEARDOWN_EVENT:
    // signals loss of a connection within existing session
    if (tdata) {

      NetworkXioClientData* cd = (NetworkXioClientData*)tdata;
      cd->conn_state = 2;
      if (cd->ncd_refcnt != 0) {
    	GLOG_INFO("portal=" << cd->pt_->coreId_ 
            << " deferring connection teardown since clientdata=" << (void*)cd 
            << " has unsent replies=" << cd->ncd_refcnt
            << " msg list size=" << cd->ncd_done_reqs.size());
      } else {
        assert(cd->ncd_conn == event_data->conn);
        xio_connection_destroy(cd->ncd_conn);
        cd->pt_->changeNumConnections(-1);
    	  GLOG_INFO("portal=" << cd->pt_->coreId_ 
            << " conn=" << (void*)cd->ncd_conn
            << " delete clientData=" << (void*)cd
            << " now serving " << cd->pt_->numConnections() << " connections(down 1)");
        delete cd;
      }
    } else {
      xio_connection_destroy(event_data->conn);
   	  GLOG_INFO("destroying conn=" << event_data->conn << " thread=" << gettid());
    }
    break;
  case XIO_SESSION_TEARDOWN_EVENT:
    // loss of session from a particular client
    // there can be other clients still running
    xio_session_destroy(session);
    break;
  default:
    break;
  };
  XXExit();
  return 0;
}

void NetworkXioServer::deallocate_request(NetworkXioRequest *req) {

  if (req->op == NetworkXioMsgOpcode::ReadRsp) {

    const size_t numElems = req->reg_mem_vec.size();

    for (size_t idx = 0; idx < numElems; idx ++) {
      if (req->from_pool_vec[idx]) {
        xio_mempool_free(&req->reg_mem_vec[idx]);
      } else {
        xio_mem_free(&req->reg_mem_vec[idx]);
      }
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
    cd->pt_->changeNumConnections(-1);
    GLOG_INFO("portal=" << cd->pt_->coreId_ 
        << " thread=" << gettid()
        << " conn=" << (void*)cd->ncd_conn
        << " closing deferred conn cd=" << (void*)cd
        << " now serving " << cd->pt_->numConnections() << " connections(down 1)");
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
      const_cast<void *>(reinterpret_cast<const void *>(req->msgpackBuffer.c_str()));
  req->xio_reply.out.header.iov_len = req->msgpackBuffer.length();

  size_t numElems = 0;
  if (req->op == NetworkXioMsgOpcode::ReadRsp) {

    numElems = req->reg_mem_vec.size();
    // Allocate the scatter-gather list upto numElems
    // TODO check numElems does not exceed configured MAX_AIO_BATCH_SIZE
    struct xio_iovec_ex* out_sglist = (struct xio_iovec_ex*)
        (calloc(numElems, sizeof(struct xio_iovec_ex)));
    req->xio_reply.out.pdata_iov.sglist = out_sglist;
    req->xio_reply.out.sgl_type = XIO_SGL_TYPE_IOV_PTR;
    vmsg_sglist_set_nents(&req->xio_reply.out, numElems);
    req->xio_reply.out.pdata_iov.max_nents = numElems;

    for (size_t idx = 0; idx < numElems; idx ++) {
      out_sglist[idx].iov_base = req->reg_mem_vec[idx].addr;
      out_sglist[idx].iov_len = req->reg_mem_vec[idx].length;
      out_sglist[idx].mr = req->reg_mem_vec[idx].mr;
    }
  }
  req->xio_reply.flags = XIO_MSG_FLAG_IMM_SEND_COMP;

  GLOG_INFO("sent reply=" << (void*)&req->xio_reply 
        << " for original message=" << (void*)xio_req
        << " req=" << (void*)req
        << " with num elem=" << numElems);

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
    for (auto& elem : ptVec_) {
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
