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

static int dbg_xio_session_destroy(xio_session* s)
{
  GLOG_INFO("destroying session=" << (void*)s);
  return xio_session_destroy(s);
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

/**
 * event handler for evfd
 * gets called by accelio 
 */
template <class T>
static void static_evfd_stop_loop(int fd, int events, void *data) {
  T *obj = reinterpret_cast<T *>(data);
  if (obj == NULL) {
    return;
  }
  obj->evfd_stop_loop(fd, events, data);
}

/** 
 * called from accelio event loop
 */
template <class T> 
static void static_timer_event(int fd, int events, void *data) {
  T *obj = reinterpret_cast<T *>(data);
  if (obj == NULL) {
    return;
  }
  obj->runTimerHandler();
}

void NetworkXioClient::runTimerHandler() {
  uint64_t count = 0;
  flushTimerFD_->recv(count);

  // move outstanding requests to flush queue
  ClientMsg* sendMsgPtr = nullptr;
  {
    std::unique_lock<std::mutex> l(currentMsgMutex);
    sendMsgPtr = currentMsgPtr;
    currentMsgPtr = nullptr;
  }
  this->flush(sendMsgPtr);
    
  size_t qsz = 0;
  if ((qsz = inflight_queue_size()) > 0) { 
    stats.timeout_queue_len = qsz;
    xstop_loop();
  }
}

/**
 * the request itself could get freed after signaling completion in
 * aio_complete_request
 * therefore, update_stats MUST be called before this function
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


std::string NetworkXioClient::statistics::ToString() const {
  std::ostringstream s;

  s 
    << " direct_queue_len=" << direct_queue_len << ",timeout_queue_len=" << timeout_queue_len
    << " num_queued=" << num_queued << ",num_failed=" << num_failed
    << ",num_completed(incl. failed)=" << num_completed
    << ",rtt_hist=" << rtt_hist << ",rtt_stats=" << rtt_stats;

  return s.str();
}

NetworkXioClient::NetworkXioClient(const uint64_t queueDepth, const std::string& uri)
    : uri_(uri)
    , stopping(false), stopped(false)
    , maxBatchSize_(queueDepth/4)
    , maxQueued_(queueDepth), inflight_reqs(queueDepth) {

  if (maxBatchSize_ == 0) {
    maxBatchSize_ = 1;
  }

  ses_ops.on_session_event = static_on_session_event<NetworkXioClient>;
  ses_ops.on_session_established = NULL;
  ses_ops.on_msg = static_on_response<NetworkXioClient>;
  ses_ops.on_msg_error = static_on_msg_error<NetworkXioClient>;
  ses_ops.on_cancel_request = NULL;
  ses_ops.assign_data_in_buf = NULL;

  bzero(&sparams, sizeof(sparams));
  bzero(&cparams, sizeof(cparams));

  xrefcnt_init();

  sparams.type = XIO_SESSION_CLIENT;
  sparams.ses_ops = &ses_ops;
  sparams.user_context = this;
  sparams.uri = uri_.c_str();

  session_ = std::shared_ptr<xio_session>(xio_session_create(&sparams),
                                         dbg_xio_session_destroy);

  GLOG_INFO("session=" << (void*)session_.get() << " created for " << uri << " by thread=" << gettid());

  if (session_ == nullptr) {
    throw XioClientCreateException("failed to create session for uri=" + uri);
  }

  std::promise<bool> promise;
  std::future<bool> future(promise.get_future());

  try {
    xio_thread_ = std::thread([&]() {
      try {
        run(promise);
      } catch (...) {
        try {
          promise.set_exception(std::current_exception());
        } catch (...) {
        }
      }
    });
  } catch (const std::system_error &) {
    throw XioClientCreateException("failed to create XIO worker thread");
  }

  try {
    future.get();
  } catch (const std::exception &) {
    xio_thread_.join();
    throw XioClientCreateException("failed to create XIO worker thread");
  }
}

void NetworkXioClient::run(std::promise<bool>& promise) {
  int ret = 0;

  int xopt = maxBatchSize_;

  ret = xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_MAX_IN_IOVLEN, &xopt,
              sizeof(xopt));
  if (ret != 0) {
    int xopt_len = 0;
    ret = xio_get_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_MAX_IN_IOVLEN, &xopt,
              &xopt_len);
    if (ret == 0) {
      maxBatchSize_ = std::min(maxBatchSize_, (size_t)xopt);
    }
  }

  ret = xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_MAX_OUT_IOVLEN, &xopt,
              sizeof(xopt));
  if (ret != 0) {
    int xopt_len = 0;
    ret = xio_get_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_MAX_IN_IOVLEN, &xopt,
              &xopt_len);
    if (ret == 0) {
      maxBatchSize_ = std::min(maxBatchSize_, (size_t)xopt);
    }
  }

  GLOG_INFO("max number of aio_readv requests packed in one accelio transport request=" << maxBatchSize_);

  xopt = 0;
  ret = xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_ENABLE_FLOW_CONTROL,
              &xopt, sizeof(xopt));

  // temporary - for debugging
  ret = xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_ENABLE_KEEPALIVE,
              &xopt, sizeof(xopt));

  xopt = 2 * maxQueued_;
  ret = xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_SND_QUEUE_DEPTH_MSGS,
              &xopt, sizeof(xopt));

  ret = xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_RCV_QUEUE_DEPTH_MSGS,
              &xopt, sizeof(xopt));

  xopt = 1;
  ret = xio_set_opt(NULL, XIO_OPTLEVEL_TCP, XIO_OPTNAME_TCP_NO_DELAY,
              &xopt, sizeof(xopt));

  // assume each request takes 1024 byte header
  xopt = maxBatchSize_ * MAX_INLINE_HEADER_OR_DATA; 
  ret = xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_MAX_INLINE_XIO_HEADER,
              &xopt, sizeof(xopt));

  ret = xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_MAX_INLINE_XIO_DATA,
              &xopt, sizeof(xopt));

  /** 
   * set environment variable XIO_TRACE = [1-6] (higher is more log)
   * or in program
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

  if (xio_context_add_ev_handler(ctx.get(), evfd, XIO_POLLIN,
                                 static_evfd_stop_loop<NetworkXioClient>,
                                 this)) {
    throw FailedRegisterEventHandler("failed to register event handler");
  }

  try {
    const uint64_t timerSec = 0;
    const uint64_t timerMicrosec = getenv_with_default("GOBJFS_CLIENT_TIMER_USEC", 50);
    flushTimerFD_ = gobjfs::make_unique<gobjfs::os::TimerNotifier>(timerSec, timerMicrosec * 1000);

    const int timerfd = flushTimerFD_->getFD();
    assert (timerfd >= 0);
    if (xio_context_add_ev_handler(ctx.get(), flushTimerFD_->getFD(), XIO_POLLIN,
                               static_timer_event<NetworkXioClient>,
                               this)) {
      throw FailedRegisterEventHandler("failed to register timer handler");
    }
  } catch (const std::exception& e) {
    throw e;
  }

  auto fp = std::bind(&NetworkXioClient::run_loop_worker, this);
  pthread_setname_np(pthread_self(), "run_loop_worker");
  promise.set_value(true);
  fp(this);
}


/** 
 * Open new connection to same URI
 * Take advantage of portals on server
 */
int NetworkXioClient::connect() {

  cparams.session = session_.get();
  cparams.ctx = ctx.get();
  cparams.conn_user_context = this;
  cparams.conn_idx = 0; 

  auto conn = xio_connect(&cparams);
  if (conn == nullptr) {
    throw XioClientCreateException("failed to connect to uri=" + uri_);
  }

  connVec.push_back(conn);

  return 0;
}

/**
 * called by external thread
 */
void NetworkXioClient::shutdown() {

  if (not stopped) {
    stopping = true;

    xstop_loop();

    xio_thread_.join();

    stopped = true;
  }

}

NetworkXioClient::~NetworkXioClient() { 
  shutdown(); 
}

bool NetworkXioClient::is_disconnected(int uri_slot) {
  return (connVec.at(uri_slot) == nullptr);
}

/**
 * called by internal thread to check for new requests
 */
int32_t NetworkXioClient::inflight_queue_size() {
  return numQueued_;
}

NetworkXioClient::ClientMsg *NetworkXioClient::pop_request() {
  ClientMsg *req = nullptr;
  bool popReturn = inflight_reqs.pop(req);
  if (popReturn == true) {
    numQueued_ --;
    if (waiters_) {
      // in queue near-full conditions, notification may not be sent
      // and pushing thread waits some extra microsec
      std::unique_lock<std::mutex> l(inflight_mutex);
      inflight_cond.notify_one();
    }
  }
  return req;
}

/**
 * called by external user thread to insert new requests
 */
void NetworkXioClient::push_request(ClientMsg *req) {


  bool pushReturn = false;
  do {
    numQueued_ ++;
    // increment before push, decrement after pop
    // invariant : queue has less than or equal to numQueued_
    pushReturn = inflight_reqs.push(req);

    if (pushReturn == false) {
      numQueued_ --;
      while (numQueued_ >= maxQueued_) {
        // TODO call stop loop here ?
        std::unique_lock<std::mutex> l(inflight_mutex);
        waiters_ ++;
        inflight_cond.wait_for(l, std::chrono::microseconds(200));
        waiters_ --;
      }
    }
  } while (pushReturn == false);

  stats.num_queued++;
  // race conditions - stat can be inaccurate
  stats.direct_queue_len = numQueued_;  
}

/**
 * wakeup the thread running the event loop
 * called by external user threads
 */
void NetworkXioClient::xstop_loop() { 
  evfd.writefd(); 
}

/**
 * event handler for evfd
 */
void NetworkXioClient::evfd_stop_loop(int fd, int /*events*/, void * /*data*/) {
  evfd.readfd();
  xio_context_stop_loop(ctx.get());
}

void NetworkXioClient::run_loop_worker() {

  GLOG_INFO("thread=" << gettid() << " running loop on ctx=" << (void*)this->ctx.get());

  timeout_ms = XIO_INFINITE;
  while (not stopping) {
    int ret = xio_context_run_loop(this->ctx.get(), timeout_ms);
    assert(ret == 0);

    int numProcessed = 0;
    while (1) {
      ClientMsg *req = pop_request();
      if (!req) { 
        break;
      }
      send_msg(req);
      numProcessed ++;
    }
    // if there were outgoing requests, we are expecting more
    // lets set timeout to zero 
    if (numProcessed) {
      timeout_ms = 0;
    } else {
      timeout_ms = XIO_INFINITE;
    }
  }

  // do the final shutdown 
  xio_context_del_ev_handler(ctx.get(), evfd);

  const int timerfd = flushTimerFD_->getFD();
  if (timerfd >= 0) {
    xio_context_del_ev_handler(ctx.get(), timerfd);
  }

  for (auto conn : connVec) { 
    if (!conn) {
      // if conn already shutdown, entry will be zero
      continue;
    }
    GLOG_INFO("thread=" << gettid() << " disconnecting conn=" << conn);
    xio_disconnect(conn);

    decltype(connVec.begin()) iter;
    do {
      iter = std::find(connVec.begin(), connVec.end(), conn);
      if (*iter != nullptr) {
        // if conn found in vector, 
        // then connection teardown event not received yet
        // so keep looping
        xio_context_run_loop(ctx.get(), 1);
      } else {
        break;
      }
    } while (*iter != nullptr);
  }

  const size_t unsentReq = inflight_queue_size();
  if (unsentReq) {
    GLOG_ERROR("queue had unsent requests=" << unsentReq);
    while (inflight_queue_size() > 0) {
      ClientMsg *req = pop_request();
      delete req;
    }
  }

  GLOG_INFO("destroying session=" << session_.get() << " use_count=" << session_.use_count());
  session_.reset();
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

  } else /* XIO_MSG_DIRECTION_IN */
  {
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
  
      void* aio_req = const_cast<void *>(msgPtr->aioReqVec_[idx]);
      update_stats(aio_req, true);
      aio_complete_request(aio_req,
          responseHeader.retvalVec_[idx], 
          responseHeader.errvalVec_[idx]);
    }
  }

  delete msgPtr;
  return ret;
}

int NetworkXioClient::on_session_event(xio_session *session
                                       __attribute__((unused)),
                                       xio_session_event_data *event_data) {
  switch (event_data->event) {

  case XIO_SESSION_CONNECTION_DISCONNECTED_EVENT:
  case XIO_SESSION_ERROR_EVENT:
  case XIO_SESSION_CONNECTION_ERROR_EVENT:
  case XIO_SESSION_CONNECTION_REFUSED_EVENT:
    break; // for debugging 

  case XIO_SESSION_CONNECTION_TEARDOWN_EVENT: {

    auto iter = std::find(connVec.begin(), connVec.end(), event_data->conn);
    if (iter != connVec.end()) {
      *iter = nullptr;
    } else {
      GLOG_ERROR("conn=" << event_data->conn << " not found");
    }
    GLOG_INFO("thread=" << gettid() << " destroying conn=" << event_data->conn);
    xio_connection_destroy(event_data->conn);
    break;

  }

  case XIO_SESSION_TEARDOWN_EVENT: {

    xio_context_stop_loop(ctx.get());
    break;
  }
  default:
    break;
  };
  return 0;
}


void NetworkXioClient::send_msg(ClientMsg *msgPtr) {
  int ret = 0;
  // TODO add check if uri_slot exists 
  do {
    ret = xio_send_request(connVec.at(msgPtr->connSlot), &msgPtr->xreq);
    // TODO resend on approp xio_errno
    NetworkXioMsg& requestHeader = msgPtr->msg;
    const size_t numElem = requestHeader.numElems_;
    {
      if (ret < 0) { 
        for (size_t idx = 0; idx < numElem; idx ++) {
          void* aio_req = const_cast<void *>(msgPtr->aioReqVec_[idx]);
          update_stats(aio_req, true);
          aio_complete_request(aio_req, -1, EIO);
        }
        delete msgPtr;
      } 
    } 
  } while (0);
}

NetworkXioClient::ClientMsg* NetworkXioClient::allocClientMsg(int32_t conn_slot) {

    ClientMsg* newMsgPtr = new ClientMsg(conn_slot);

    // TODO have preallocated msgptr
    bzero(static_cast<void *>(&newMsgPtr->xreq), sizeof(xio_msg));

    NetworkXioMsg* requestHeader = &newMsgPtr->msg;
    requestHeader->opcode(NetworkXioMsgOpcode::ReadReq);
    requestHeader->clientMsgPtr_ = (uintptr_t)newMsgPtr;
    requestHeader->numElems_ = 0;

    requestHeader->sizeVec_.reserve(maxBatchSize_);
    requestHeader->offsetVec_.reserve(maxBatchSize_);
    requestHeader->filenameVec_.reserve(maxBatchSize_);
    requestHeader->errvalVec_.reserve(maxBatchSize_);
    requestHeader->retvalVec_.reserve(maxBatchSize_);

    newMsgPtr->aioReqVec_.reserve(maxBatchSize_);

    newMsgPtr->xreq.in.sgl_type = XIO_SGL_TYPE_IOV_PTR;
    newMsgPtr->xreq.in.pdata_iov.max_nents = maxBatchSize_;
    newMsgPtr->xreq.in.pdata_iov.sglist = (xio_iovec_ex*)
        (calloc(maxBatchSize_, sizeof(xio_iovec_ex)));

    xio_iovec_ex *in_sglist = newMsgPtr->xreq.in.pdata_iov.sglist;
    for (size_t idx = 0; idx < maxBatchSize_; idx ++) {
      in_sglist[idx].iov_base = nullptr;
      in_sglist[idx].iov_len = 0;
    }
    return newMsgPtr;
}

int NetworkXioClient::flush(ClientMsg* sendMsgPtr)
{
  if (sendMsgPtr) {
    sendMsgPtr->prepare();
    vmsg_sglist_set_nents(&sendMsgPtr->xreq.in, sendMsgPtr->msg.numElems_);
    push_request(sendMsgPtr);
    if (timeout_ms != 0) {
      xstop_loop();
    }
  }
  return 0;
}

int NetworkXioClient::append_read_request(const std::string &filename,
                                             void *buf,
                                             const uint64_t size_in_bytes,
                                             const uint64_t offset_in_bytes,
                                             void *aioReqPtr) {

  std::unique_lock<std::mutex> l(currentMsgMutex);

  if (!currentMsgPtr) {
    currentMsgPtr = allocClientMsg(0); // TODO use portals
  }
  NetworkXioMsg* requestHeader = &currentMsgPtr->msg;

  assert(requestHeader->numElems_ < maxBatchSize_);

  // TODO reserve vector size
  currentMsgPtr->aioReqVec_.push_back(aioReqPtr);
  requestHeader->sizeVec_.push_back(size_in_bytes);
  requestHeader->offsetVec_.push_back(offset_in_bytes);
  requestHeader->filenameVec_.push_back(filename);

  const size_t curIdx = requestHeader->numElems_;

  xio_iovec_ex *in_sglist = currentMsgPtr->xreq.in.pdata_iov.sglist;
  in_sglist[curIdx].iov_base = buf;
  in_sglist[curIdx].iov_len = size_in_bytes;

  requestHeader->numElems_ ++;

  if (requestHeader->numElems_ == maxBatchSize_) {
    ClientMsg* sendMsgPtr = currentMsgPtr;
    currentMsgPtr = nullptr;
    l.unlock();

    // flush outside the mutex, lets other threads append
    this->flush(sendMsgPtr); 
  } 

  return 0;
}

int NetworkXioClient::on_response(xio_session *session __attribute__((unused)),
                                  xio_msg *reply,
                                  int last_in_rxq __attribute__((unused))) {
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
  
      void* aio_req = msgPtr->aioReqVec_[idx];
      update_stats(aio_req, (responseHeader.retvalVec_[idx] < 0));
      aio_complete_request(aio_req,
                                responseHeader.retvalVec_[idx], 
                                responseHeader.errvalVec_[idx]);
    }

    // free memory allocated to sglist
    if (msgPtr->xreq.in.sgl_type == XIO_SGL_TYPE_IOV_PTR) {
      free(vmsg_sglist(&msgPtr->xreq.in));
    }

    // the msgPtr must be freed after xio_release_response()
    // otherwise its a memory corruption bug
    delete msgPtr;
  }
 
  return 0;
}

void NetworkXioClient::ClientMsg::prepare() {
  msgpackBuffer = msg.pack_msg();

  vmsg_sglist_set_nents(&xreq.out, 0);
  xreq.out.header.iov_base = (void *)msgpackBuffer.c_str();
  xreq.out.header.iov_len = msgpackBuffer.length();
}

}
} // namespace gobjfs
