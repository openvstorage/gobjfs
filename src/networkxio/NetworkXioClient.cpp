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

#include <thread>
#include <future>
#include <functional>
#include <atomic>
#include <system_error>
#include <chrono>

#include <sstream>

#include <networkxio/gobjfs_client_common.h>
#include <networkxio/gobjfs_config.h>
#include <gobjfs_client.h>
#include "NetworkXioClient.h"
#include "NetworkXioCommon.h"
#include "gobjfs_getenv.h"

namespace gobjfs {
namespace xio {

/**
 * keep one session per uri for portals
 */

typedef std::map<std::string, std::shared_ptr<xio_session>> SessionMap;

static SessionMap sessionMap;
static std::mutex mutex;

static int dbg_xio_session_destroy(xio_session* s)
{
    GLOG_INFO("destroying session=" << (void*)s << " for uri="  << getURI(s));
    return xio_session_destroy(s);
}

static inline std::shared_ptr<xio_session> getSession(const std::string& uri, 
    xio_session_params& session_params) {

  std::shared_ptr<xio_session> retptr;
  std::unique_lock<std::mutex> l(mutex);

  auto iter = sessionMap.find(uri);
  if (iter == sessionMap.end()) {
    // session does not exist, create one
    auto session = std::shared_ptr<xio_session>(xio_session_create(&session_params),
                                         dbg_xio_session_destroy);
#ifdef SHARED_SESSION
    //TODO change conn_idx = 0 when you enable this code
    auto insertIter = sessionMap.insert(std::make_pair(uri, session));
    assert(insertIter.second == true); // insert succeded
#endif
    GLOG_INFO("session=" << (void*)session.get() << " created for " << uri);
    retptr = session;
  } else {
    retptr = iter->second;
    GLOG_INFO("session=" << (void*)retptr.get() << " found for " << uri);
  }
  return retptr;
}

/**
 * initialize xio once
 */
static int xio_init_refcnt = 0;
static std::mutex xioLibMutex;

static inline void xrefcnt_init() {
  std::unique_lock<std::mutex> l(xioLibMutex);
  if (++ xio_init_refcnt == 1) {
    xio_init();
    GLOG_INFO("starting up xio lib");
  }
}

static inline void xrefcnt_shutdown() {
  std::unique_lock<std::mutex> l(xioLibMutex);
  if (-- xio_init_refcnt == 0) {
    GLOG_INFO("shutting down xio lib");
    xio_shutdown();
  }
}

static void destroy_ctx_shutdown(xio_context *ctx) {
  xio_context_destroy(ctx);
  xrefcnt_shutdown();
}

template <class T>
static int static_on_session_event(xio_session *session,
                                   xio_session_event_data *event_data,
                                   void *cb_user_context) {
  T *obj = reinterpret_cast<T *>(cb_user_context);
  if (obj == NULL) {
    return -1;
  }

  std::string peerAddr;
  int peerPort = -1;
  std::string localAddr;
  int localPort = -1;
  // causing a SEGV
  //getAddressAndPort(event_data->conn, localAddr, localPort, peerAddr, peerPort);

  GLOG_INFO("got session event=" << xio_session_event_str(event_data->event)
      << ",reason=" << xio_strerror(event_data->reason)
      << ",conn=" << event_data->conn
      << ",cb_user_ctx=" << (void*)cb_user_context
      << ",thread=" << gettid() 
      << ",peer_addr=" << peerAddr.c_str()
      << ",peer_port=" << peerPort
      << ",local_addr=" << localAddr.c_str()
      << ",local_port=" << localPort
      << ",uri=" << getURI(session));

  return obj->on_session_event(session, event_data);
}

template <class T>
static int static_on_response(xio_session *session, xio_msg *req,
                              int last_in_rxq, void *cb_user_context) {
  T *obj = reinterpret_cast<T *>(cb_user_context);
  if (obj == NULL) {
    return -1;
  }
  return obj->on_response(session, req, last_in_rxq);
}

template <class T>
static int static_on_msg_error(xio_session *session, xio_status error,
                               xio_msg_direction direction, xio_msg *msg,
                               void *cb_user_context) {
  T *obj = reinterpret_cast<T *>(cb_user_context);
  if (obj == NULL) {
    return -1;
  }
  return obj->on_msg_error(session, error, direction, msg);
}

std::string NetworkXioClient::statistics::ToString() const {
  std::ostringstream s;

  s << " num_queued=" << num_queued << ",num_failed=" << num_failed
    << ",num_completed(incl. failed)=" << num_completed
    << ",rtt_hist=" << rtt_hist << ",rtt_stats=" << rtt_stats;

  return s.str();
}

NetworkXioClient::NetworkXioClient(const std::string &uri, const uint64_t qd)
    : uri_(uri), stopping(false), stopped(false), disconnected(false),
      disconnecting(false), maxQueueLen_(qd) {
  XXEnter();

  ses_ops.on_session_event = static_on_session_event<NetworkXioClient>;
  ses_ops.on_session_established = NULL;
  ses_ops.on_msg = static_on_response<NetworkXioClient>;
  ses_ops.on_msg_error = static_on_msg_error<NetworkXioClient>;
  ses_ops.on_cancel_request = NULL;
  ses_ops.assign_data_in_buf = NULL;

  bzero(&sparams, sizeof(sparams));
  bzero(&cparams, sizeof(cparams));

  sparams.type = XIO_SESSION_CLIENT;
  sparams.ses_ops = &ses_ops;
  sparams.user_context = this;
  sparams.uri = uri_.c_str();

  xrefcnt_init();

  run();
}

void NetworkXioClient::run() {
  int xopt = 0;
  xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_ENABLE_FLOW_CONTROL,
              &xopt, sizeof(xopt));

  xopt = maxQueueLen_;
  xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_SND_QUEUE_DEPTH_MSGS,
              &xopt, sizeof(xopt));

  xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_RCV_QUEUE_DEPTH_MSGS,
              &xopt, sizeof(xopt));

  xopt = 1;
  xio_set_opt(NULL, XIO_OPTLEVEL_TCP, XIO_OPTNAME_TCP_NO_DELAY,
              &xopt, sizeof(xopt));

  /** 
   * set env var XIO_TRACE = 4 (XIO_LOG_LEVEL_DEBUG)
   * xopt = XIO_LOG_LEVEL_DEBUG;
   * xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_LOG_LEVEL,
   *            &xopt, sizeof(xopt));
   */

  try {
    const int polling_time_usec = getenv_with_default("GOBJFS_POLLING_TIME_USEC", POLLING_TIME_USEC_DEFAULT);

    ctx = std::shared_ptr<xio_context>(
        xio_context_create(NULL, polling_time_usec, -1),
        destroy_ctx_shutdown);
  } catch (const std::bad_alloc &) {
    xrefcnt_shutdown();
    throw;
  }

  if (ctx == nullptr) {
    throw XioClientCreateException("failed to create XIO context");
  }

  //session = std::shared_ptr<xio_session>(xio_session_create(&params),
                                         //xio_session_destroy);
  session = getSession(uri_, sparams);
  if (session == nullptr) {
    throw XioClientCreateException("failed to create XIO client");
  }

  cparams.session = session.get();
  cparams.ctx = ctx.get();
  cparams.conn_user_context = this;
#ifdef SHARED_SESSION
  cparams.conn_idx = 0; 
  // xio will auto-increment internally when conn_idx = 0
  // and load-balance when session is shared between multiple connections
  // enable this code after fixing shared session destructor error
#else
  // this is temporary hack assuming max portals never more than 20
  cparams.conn_idx = gettid() % 20;
#endif

  conn = xio_connect(&cparams);
  if (conn == nullptr) {
    throw XioClientCreateException("failed to connect");
  }
}

/**
 * can only be called from the thread owning xio_context
 */
void NetworkXioClient::shutdown() {
  GLOG_INFO("thread=" << gettid() << " disconnecting conn=" << conn);
  xio_disconnect(conn);
  if (not disconnected) {
    disconnecting = true;
    xio_context_run_loop(ctx.get(), XIO_INFINITE);
  } else {
    GLOG_INFO("thread=" << gettid() << " destroying conn=" << conn);
    xio_connection_destroy(conn);
  }
  //session.reset(); TODO
}

NetworkXioClient::~NetworkXioClient() { 
  shutdown(); 
}


/**
 * the request can get freed after signaling completion in
 * aio_complete_request
 * therefore, update_stats must be called before this function
 * otherwise its a use-after-free error
 */
void NetworkXioClient::update_stats(aio_request *req, bool req_failed) {
  if (!req) {
    stats.num_failed++;
    return;
  }

  req->_rtt_nanosec = req->_timer.elapsedNanoseconds();
  if (req_failed) {
    stats.num_failed++;
  }
  stats.num_completed++;
  stats.rtt_hist = req->_rtt_nanosec;
  stats.rtt_stats = req->_rtt_nanosec;
}

int NetworkXioClient::on_msg_error(xio_session *session __attribute__((unused)),
                                   xio_status error __attribute__((unused)),
                                   xio_msg_direction direction, xio_msg *msg) {
  int ret = 0;

  NetworkXioMsg responseHeader;

  if (direction == XIO_MSG_DIRECTION_OUT) {
    try {
      responseHeader.unpack_msg(static_cast<const char *>(msg->out.header.iov_base),
                      msg->out.header.iov_len);
    } catch (...) {
      GLOG_ERROR("failed to unpack msg");
      return 0;
    }
    // free memory allocated to sglist
    if (msg->out.sgl_type == XIO_SGL_TYPE_IOV_PTR) {
      free(vmsg_sglist(&msg->out));
    }
  } else {
    /* XIO_MSG_DIRECTION_IN */
    try {
      responseHeader.unpack_msg(static_cast<const char *>(msg->in.header.iov_base),
                      msg->in.header.iov_len);
    } catch (...) {
      xio_release_response(msg);
      GLOG_ERROR("failed to unpack msg");
      return 0;
    }
    msg->in.header.iov_base = NULL;
    msg->in.header.iov_len = 0;
    vmsg_sglist_set_nents(&msg->in, 0);
    xio_release_response(msg);

    // free memory allocated to sglist
    if (msg->in.sgl_type == XIO_SGL_TYPE_IOV_PTR) {
      free(vmsg_sglist(&msg->in));
    }
  }

  const size_t numElem = responseHeader.numElems_;
  if ((numElem != responseHeader.errvalVec_.size()) || 
      (numElem != responseHeader.retvalVec_.size())) {
    GLOG_ERROR("num elements in header=" << numElem << " not matching "
        << "num elements in retval=" << responseHeader.retvalVec_.size() 
        << " or num elements in errval=" << responseHeader.errvalVec_.size());
    ret = -1;
  }

  ClientMsg *msgPtr = reinterpret_cast<ClientMsg *>(responseHeader.clientMsgPtr_);

  if (ret == 0) {

    for (size_t idx = 0; idx < numElem; idx ++) {
  
      auto aio_req = msgPtr->aioReqVec_[idx];
      update_stats(aio_req, true);
      aio_complete_request(aio_req,
          responseHeader.retvalVec_[idx], 
          responseHeader.errvalVec_[idx]);
    }
  }

  inFlightQueueLen_ --;

  delete msgPtr;
  xio_context_stop_loop(ctx.get());
  return ret;
}

int NetworkXioClient::on_session_event(xio_session *session
                                       __attribute__((unused)),
                                       xio_session_event_data *event_data) {
  XXEnter();
  switch (event_data->event) {

  case XIO_SESSION_CONNECTION_DISCONNECTED_EVENT:
  case XIO_SESSION_ERROR_EVENT:
  case XIO_SESSION_CONNECTION_ERROR_EVENT:
  case XIO_SESSION_CONNECTION_REFUSED_EVENT:
    break; // for debugging

  case XIO_SESSION_CONNECTION_TEARDOWN_EVENT:
    xio_connection_destroy(event_data->conn);
    disconnected = true;
    break;

  case XIO_SESSION_TEARDOWN_EVENT:
    xio_context_stop_loop(ctx.get());
    break;
  default:
    break;
  };
  XXExit();
  return 0;
}

void NetworkXioClient::run_loop(int timeout_ms) {
  int ret = xio_context_run_loop(ctx.get(), timeout_ms);
  assert(ret == 0);
}

int NetworkXioClient::send_msg(ClientMsg *msgPtr) {
  int ret = 0;
 
  if (inFlightQueueLen_ < maxQueueLen_) {
    // process any finished requests before submitting new
    run_loop(0);
  } else {
    int numTries = 0;
    while (inFlightQueueLen_ == maxQueueLen_) {
      // will exit on every on_response, on_msg_error called
      run_loop();
      numTries ++;
      if (numTries > 3) {
        break;
      }
    }
    if (inFlightQueueLen_ == maxQueueLen_) {
      GLOG_ERROR("queue full sent=" << inFlightQueueLen_ << " max=" << maxQueueLen_);
      ret = -1;
    }
  }

  if (ret == 0) {
    ret = xio_send_request(conn, &msgPtr->xreq);
  }

  if (ret < 0) { 
    NetworkXioMsg& requestHeader = msgPtr->msg;
    const size_t numElem = requestHeader.numElems_;
    for (size_t idx = 0; idx < numElem; idx ++) {
      auto aio_req = msgPtr->aioReqVec_[idx];
      update_stats(aio_req, true);
      aio_complete_request(aio_req, -1, EIO);
    }
    delete msgPtr;
  } else {
    stats.num_queued += msgPtr->aioReqVec_.size();
    inFlightQueueLen_ ++;
  }

  return ret;
}

int NetworkXioClient::send_multi_read_request(std::vector<std::string> &&filenameVec, 
    std::vector<void *>   &&bufVec,
    std::vector<uint64_t> &&sizeVec,
    std::vector<uint64_t> &&offsetVec,
    const std::vector<aio_request *>   &aioReqVec) {


  ClientMsg *msgPtr = new ClientMsg;
  msgPtr->aioReqVec_ = aioReqVec;

  NetworkXioMsg& requestHeader = msgPtr->msg;
  requestHeader.opcode(NetworkXioMsgOpcode::ReadReq);
  requestHeader.clientMsgPtr_ = reinterpret_cast<uintptr_t>(msgPtr);

  const size_t numElems = filenameVec.size();
  requestHeader.numElems_ = numElems;

  // Caller must ensure
  // all vectors have same size
  // AND batch size less than max configured
  assert(numElems <= maxBatchSize_); 
  assert(bufVec.size() == numElems); 
  assert(sizeVec.size() == numElems);
  assert(offsetVec.size() == numElems);
  assert(aioReqVec.size() == numElems);

  requestHeader.sizeVec_ = std::move(sizeVec);
  requestHeader.offsetVec_ = std::move(offsetVec);
  requestHeader.filenameVec_ = std::move(filenameVec);

  msgPtr->prepare();

  // Allocate the scatter-gather list upto numElems
  msgPtr->xreq.in.sgl_type = XIO_SGL_TYPE_IOV_PTR;
  vmsg_sglist_set_nents(&msgPtr->xreq.in, numElems);
  msgPtr->xreq.in.pdata_iov.max_nents = numElems;
  struct xio_iovec_ex* in_sglist = (struct xio_iovec_ex*)
      (calloc(numElems, sizeof(struct xio_iovec_ex)));
  msgPtr->xreq.in.pdata_iov.sglist = in_sglist;
  for (size_t idx = 0; idx < numElems; idx ++) {
    in_sglist[idx].iov_base = bufVec[idx];
    in_sglist[idx].iov_len = requestHeader.sizeVec_[idx];
  }

  bufVec.clear();
  // check the vectors have been "std::move" and emptied 
  assert(filenameVec.empty());
  assert(sizeVec.empty());
  assert(offsetVec.empty());

  return send_msg(msgPtr);
}


int NetworkXioClient::send_read_request(const std::string &filename,
                                             void *buf,
                                             const uint64_t size_in_bytes,
                                             const uint64_t offset_in_bytes,
                                             aio_request *aioReqPtr) {
  XXEnter();

  ClientMsg *msgPtr = new ClientMsg;
  msgPtr->aioReqVec_.push_back(aioReqPtr);

  NetworkXioMsg* requestHeader = &msgPtr->msg;
  requestHeader->opcode(NetworkXioMsgOpcode::ReadReq);
  requestHeader->clientMsgPtr_ = (uintptr_t)msgPtr;
  requestHeader->numElems_ = 1;

  requestHeader->sizeVec_.push_back(size_in_bytes);
  requestHeader->offsetVec_.push_back(offset_in_bytes);
  requestHeader->filenameVec_.push_back(filename);

  msgPtr->prepare();

  // TODO Check on server if data in IOV_PTR or not
  vmsg_sglist_set_nents(&msgPtr->xreq.in, 1);
  msgPtr->xreq.in.data_iov.sglist[0].iov_base = buf;
  msgPtr->xreq.in.data_iov.sglist[0].iov_len = size_in_bytes;

  return send_msg(msgPtr);
}

int NetworkXioClient::on_response(xio_session *session __attribute__((unused)),
                                  xio_msg *reply,
                                  int last_in_rxq __attribute__((unused))) {
  XXEnter();
  NetworkXioMsg responseHeader;
  try {
    responseHeader.unpack_msg(static_cast<const char *>(reply->in.header.iov_base),
                    reply->in.header.iov_len);
  } catch (...) {
    GLOG_ERROR("failed to unpack msg");
    xio_release_response(reply);
    return 0;
  }

  reply->in.header.iov_base = NULL;
  reply->in.header.iov_len = 0;
  vmsg_sglist_set_nents(&reply->in, 0);
  xio_release_response(reply);

  const size_t numElems = responseHeader.numElems_;
  {
    ClientMsg *msgPtr = reinterpret_cast<ClientMsg *>(responseHeader.clientMsgPtr_);

    for (size_t idx = 0; idx < numElems; idx ++) {
  
      auto aio_req = msgPtr->aioReqVec_[idx];
      update_stats(aio_req, (responseHeader.retvalVec_[idx] < 0));
      aio_complete_request(aio_req,
                                responseHeader.retvalVec_[idx], 
                                responseHeader.errvalVec_[idx]);
    }

    inFlightQueueLen_ --;

    // free memory allocated to sglist
    if (msgPtr->xreq.in.sgl_type == XIO_SGL_TYPE_IOV_PTR) {
      free(vmsg_sglist(&msgPtr->xreq.in));
    }

    // the msgPtr must be freed after xio_release_response()
    // otherwise its a memory corruption bug
    delete msgPtr;
    xio_context_stop_loop(ctx.get());
  }
  
  return 0;
}

NetworkXioClient::ClientMsg::ClientMsg() {
  bzero(static_cast<void *>(&xreq), sizeof(xreq));
}

void NetworkXioClient::ClientMsg::prepare() {
  msgpackBuffer = msg.pack_msg();

  vmsg_sglist_set_nents(&xreq.out, 0);
  xreq.out.header.iov_base = (void *)msgpackBuffer.c_str();
  xreq.out.header.iov_len = msgpackBuffer.length();
}
}
} // namespace gobjfs
