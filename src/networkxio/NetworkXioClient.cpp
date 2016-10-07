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
    //auto insertIter = sessionMap.insert(std::make_pair(uri, session));
    //assert(insertIter.second == true); // insert succeded
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

inline void _xio_aio_wake_up_suspended_aiocb(aio_request *request) {
  XXEnter();
  {
    request->_signaled = true;
    GLOG_DEBUG("waking up the suspended thread for request=" << (void*)request);
    request->_cvp->signal();
  }
  XXExit();
}

/* called when response is received by NetworkXioClient */
void ovs_xio_aio_complete_request(void *opaque, ssize_t retval, int errval) {
  XXEnter();
  aio_request *request = reinterpret_cast<aio_request *>(opaque);
  completion *cptr = request->cptr;
  request->_errno = errval;
  request->_rv = retval;
  request->_failed = (retval < 0 ? true : false);
  request->_completed = true;

  //_xio_aio_wake_up_suspended_aiocb(request); 

  if (cptr) {
    cptr->_rv = retval;
    cptr->_failed = (retval == -1 ? true : false);
    GLOG_DEBUG("signalling completion for request=" << (void*)request);
    // first invoke the callback, then signal completion
    // caller must free the completion in main loop - not in callback!
    cptr->complete_cb(cptr, cptr->cb_arg);
    aio_signal_completion(cptr);
  }
  XXExit();
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
      disconnecting(false), nr_req_queue(qd) {
  XXEnter();

  ses_ops.on_session_event = static_on_session_event<NetworkXioClient>;
  ses_ops.on_session_established = NULL;
  ses_ops.on_msg = static_on_response<NetworkXioClient>;
  ses_ops.on_msg_error = static_on_msg_error<NetworkXioClient>;
  ses_ops.on_cancel_request = NULL;
  ses_ops.assign_data_in_buf = NULL;

  memset(&params, 0, sizeof(params));
  memset(&cparams, 0, sizeof(cparams));

  params.type = XIO_SESSION_CLIENT;
  params.ses_ops = &ses_ops;
  params.user_context = this;
  params.uri = uri_.c_str();

  int queue_depth = 2 * nr_req_queue;

  xrefcnt_init();

  run();
}

void NetworkXioClient::run() {
  int xopt = 0;
  xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_ENABLE_FLOW_CONTROL,
              &xopt, sizeof(xopt));

  xopt = 2 * nr_req_queue;
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
        xio_destroy_ctx_shutdown);
  } catch (const std::bad_alloc &) {
    xrefcnt_shutdown();
    throw;
  }

  if (ctx == nullptr) {
    throw XioClientCreateException("failed to create XIO context");
  }

  //session = std::shared_ptr<xio_session>(xio_session_create(&params),
                                         //xio_session_destroy);
  session = getSession(uri_, params);
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
  //session.reset();
}

NetworkXioClient::~NetworkXioClient() { shutdown(); }

void NetworkXioClient::xio_destroy_ctx_shutdown(xio_context *ctx) {
  xio_context_destroy(ctx);
  xrefcnt_shutdown();
  XXExit();
}

/**
 * the request can get freed after signaling completion in
 * ovs_xio_aio_complete_request
 * therefore, update_stats must be called before this function
 * otherwise its a use-after-free error
 */
void NetworkXioClient::update_stats(void *void_req, bool req_failed) {
  aio_request *req = static_cast<aio_request *>(void_req);
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
  NetworkXioMsg imsg;
  xio_msg_s *xio_msg;
  if (direction == XIO_MSG_DIRECTION_OUT) {
    try {
      imsg.unpack_msg(static_cast<const char *>(msg->out.header.iov_base),
                      msg->out.header.iov_len);
    } catch (...) {
      GLOG_ERROR("failed to unpack msg");
      return 0;
    }
  } else /* XIO_MSG_DIRECTION_IN */
  {
    try {
      imsg.unpack_msg(static_cast<const char *>(msg->in.header.iov_base),
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
  }
  xio_msg = reinterpret_cast<xio_msg_s *>(imsg.opaque());

  update_stats(const_cast<void *>(xio_msg->opaque), true);
  ovs_xio_aio_complete_request(const_cast<void *>(xio_msg->opaque), -1, EIO);

  nr_req_queue++;
  delete xio_msg;
  return 0;
}

int NetworkXioClient::on_session_event(xio_session *session
                                       __attribute__((unused)),
                                       xio_session_event_data *event_data) {
  XXEnter();
  switch (event_data->event) {
  case XIO_SESSION_CONNECTION_DISCONNECTED_EVENT:
  case XIO_SESSION_ERROR_EVENT:
  case XIO_SESSION_CONNECTION_ERROR_EVENT:
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

void NetworkXioClient::run_loop() {
  int ret = xio_context_run_loop(ctx.get(), XIO_INFINITE);
  assert(ret == 0);
}

void NetworkXioClient::send_msg(xio_msg_s *xmsg) {
  int ret = 0;
  do {
    ret = xio_send_request(conn, &xmsg->xreq);
    // TODO resend on approp xio_errno
    // update_stats(const_cast<void *>(req->opaque), true);
    // ovs_xio_aio_complete_request(const_cast<void *>(req->opaque), -1, EIO)
    // delete req;
    if (ret < 0) { 
      update_stats(const_cast<void *>(xmsg->opaque), true);
      ovs_xio_aio_complete_request(const_cast<void *>(xmsg->opaque), -1, EIO);
      delete xmsg;
    } else {
      --nr_req_queue;
      stats.num_queued ++;
    }
  } while (0);
}

void NetworkXioClient::xio_send_open_request(const void *opaque) {
  XXEnter();
  xio_msg_s *xmsg = new xio_msg_s;
  xmsg->opaque = opaque;
  xmsg->msg.opcode(NetworkXioMsgOpcode::OpenReq);
  xmsg->msg.opaque((uintptr_t)xmsg);

  msg_prepare(xmsg);
  send_msg(xmsg);
  XXExit();
}

void NetworkXioClient::xio_send_read_request(const std::string &filename,
                                             void *buf,
                                             const uint64_t size_in_bytes,
                                             const uint64_t offset_in_bytes,
                                             const void *opaque) {
  XXEnter();
  xio_msg_s *xmsg = new xio_msg_s;
  xmsg->opaque = opaque;
  xmsg->msg.opcode(NetworkXioMsgOpcode::ReadReq);
  xmsg->msg.opaque((uintptr_t)xmsg);
  xmsg->msg.size(size_in_bytes);
  xmsg->msg.offset(offset_in_bytes);
  xmsg->msg.filename_ = filename;

  msg_prepare(xmsg);

  vmsg_sglist_set_nents(&xmsg->xreq.in, 1);
  xmsg->xreq.in.data_iov.sglist[0].iov_base = buf;
  xmsg->xreq.in.data_iov.sglist[0].iov_len = size_in_bytes;
  send_msg(xmsg);
}

int NetworkXioClient::on_response(xio_session *session __attribute__((unused)),
                                  xio_msg *reply,
                                  int last_in_rxq __attribute__((unused))) {
  XXEnter();
  NetworkXioMsg imsg;
  try {
    imsg.unpack_msg(static_cast<const char *>(reply->in.header.iov_base),
                    reply->in.header.iov_len);
  } catch (...) {
    GLOG_ERROR("failed to unpack msg");
    xio_release_response(reply);
    return 0;
  }
  xio_msg_s *xio_msg = reinterpret_cast<xio_msg_s *>(imsg.opaque());

  update_stats(const_cast<void *>(xio_msg->opaque), (imsg.retval() < 0));
  ovs_xio_aio_complete_request(const_cast<void *>(xio_msg->opaque),
                               imsg.retval(), imsg.errval());

  reply->in.header.iov_base = NULL;
  reply->in.header.iov_len = 0;
  vmsg_sglist_set_nents(&reply->in, 0);
  xio_release_response(reply);
  nr_req_queue++;
  xio_context_stop_loop(ctx.get());
  delete xio_msg;
  XXExit();
  return 0;
}

void NetworkXioClient::msg_prepare(xio_msg_s *xmsg) {
  XXEnter();
  xmsg->s_msg = xmsg->msg.pack_msg();

  memset(static_cast<void *>(&xmsg->xreq), 0, sizeof(xio_msg));

  vmsg_sglist_set_nents(&xmsg->xreq.out, 0);
  xmsg->xreq.out.header.iov_base = (void *)xmsg->s_msg.c_str();
  xmsg->xreq.out.header.iov_len = xmsg->s_msg.length();
  XXExit();
}
}
} // namespace gobjfs
