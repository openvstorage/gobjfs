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

#include <unistd.h>

#include <networkxio/gobjfs_client_common.h>
#include <gobjfs_client.h>
#include <IOExecutor.h>

#include "gobjfs_getenv.h"
#include "NetworkXioIOHandler.h"
#include "NetworkXioCommon.h"
#include "NetworkXioProtocol.h"
#include "NetworkXioServer.h"
#include "PortalThreadData.h"

#include <util/lang_utils.h>

using namespace std;
namespace gobjfs {
namespace xio {

/**
 * two things have to be done to send the response to client
 * 1. pack the header using msgpack (pack_msg)
 * 2. xio_send_response(header + data buffer)
 */
void NetworkXioRequest::pack_msg() {
  GLOG_DEBUG(" packing msg for req=" << (void*)this
      << ",retvec=" << this->retvalVec_.size()
      << ",errvec=" << this->errvalVec_.size()
      << ",client_msg=" << this->clientMsgPtr_
      );
  NetworkXioMsg replyHeader(this->op);
  replyHeader.numElems_ = this->numElems_;
  replyHeader.retvalVec_ = this->retvalVec_;
  replyHeader.errvalVec_ = this->errvalVec_;
  replyHeader.clientMsgPtr_ = this->clientMsgPtr_;
  msgpackBuffer = replyHeader.pack_msg();
}

/** 
 * called from accelio event loop
 */
template <class T> 
static void static_disk_event(int fd, int events, void *data) {
  T *obj = reinterpret_cast<T *>(data);
  if (obj == NULL) {
    return;
  }
  int numExpected = EventFD::readfd(obj->eventFD_);
  obj->ioexecPtr_->handleDiskCompletion(numExpected);
}


NetworkXioIOHandler::NetworkXioIOHandler(PortalThreadData* pt)
  : pt_(pt) {

  // just create one IOExecutor and bind it to this handler
  const int numCores = 1;

  serviceHandle_ = IOExecFileServiceInit(numCores, 
        pt_->server_->queueDepthForIO_,
        pt_->server_->fileTranslatorFunc_, 
        pt_->server_->newInstance_);

  if (serviceHandle_ == nullptr) {
    throw std::bad_alloc();
  }

  // TODO smart ptr
  eventHandle_ = IOExecEventFdOpen(serviceHandle_);

  eventFD_ = IOExecEventFdGetReadFd(eventHandle_);

  startEventHandler();
}

NetworkXioIOHandler::~NetworkXioIOHandler() {

  stopEventHandler();

  IOExecEventFdClose(eventHandle_);

  if (serviceHandle_) {
    IOExecFileServiceDestroy(serviceHandle_);
    serviceHandle_ = nullptr;
  }

}

void NetworkXioIOHandler::startEventHandler() {

  try {

    ioexecPtr_ = static_cast<IOExecutor*>(IOExecGetExecutorPtr(serviceHandle_, 0));

    if (xio_context_add_ev_handler(pt_->ctx_, eventFD_,
                                XIO_POLLIN,
                                static_disk_event<NetworkXioIOHandler>,
                                this)) {

      throw FailedRegisterEventHandler("failed to register event handler");

    }

    GLOG_INFO("registered disk event fd=" << eventFD_
        << ",ioexecutor=" << ioexecPtr_
        << ",thread=" << gettid());


  } catch (std::exception& e) {
    GLOG_ERROR("failed to init handler " << e.what());
  }
}

/** 
 * called from FilerJob::reset after submitted IO is complete
 */
static int static_runEventHandler(gIOStatus& iostatus, void* ctx) {
  NetworkXioIOHandler* handler = (NetworkXioIOHandler*)ctx;
  handler->runEventHandler(iostatus);
  return 0;
}

/**
 * this runs in the context of the portal thread
 */
int NetworkXioIOHandler::runEventHandler(gIOStatus& iostatus) {

  gIOBatch *batch = reinterpret_cast<gIOBatch *>(iostatus.completionId);
  assert(batch != nullptr);

  NetworkXioRequest *req =
      static_cast<NetworkXioRequest *>(batch->opaque);
  assert(req != nullptr);

  gIOExecFragment &frag = batch->array[0];
  // reset addr otherwise BatchFree will free it
  // need to introduce ownership indicator
  const size_t fragSize = frag.size;
  frag.addr = nullptr;
  gIOBatchFree(batch);

  switch (req->op) {

  case NetworkXioMsgOpcode::ReadRsp: {

    if (iostatus.errorCode == 0) {
      // read must return the size which was read
      req->retvalVec_.push_back(fragSize);
      req->errvalVec_.push_back(0);
      // TODO_MULTI figure out which opaque to update error code for
      GLOG_DEBUG(" Read completed with completion ID"
                 << iostatus.completionId);
    } else {
      req->retvalVec_.push_back(-1);
      req->errvalVec_.push_back(iostatus.errorCode);
      GLOG_ERROR("Read completion error " << iostatus.errorCode
                                          << " For completion ID "
                                          << iostatus.completionId);
    }

    req->completeElems_ ++;
    // if all in batch done, then pack and send reply
    if (req->completeElems_ == req->numElems_) {
      req->pack_msg();
      pt_->server_->send_reply(req);
    }

  } break;

  default: 
    GLOG_ERROR("Got an event for non-read operation "
               << (int)req->op);
  }
  return 0;
}

void NetworkXioIOHandler::stopEventHandler() {
  GLOG_INFO("deregistered disk fd=" << eventFD_ << " for thread=" << gettid());
  xio_context_del_ev_handler(pt_->ctx_, eventFD_);
}

/**
 * Called from NetworkXioServer's timer handler
 */
void NetworkXioIOHandler::runTimerHandler()
{
  if (pt_->numConnections()) { 

    // Dynamically vary minSubmitSize based on batch size
    // observed in previous second
    /*
    const auto currentBatchSize = ioexecPtr_->stats_.numProcessedInLoop_.mean_;
    if (currentBatchSize != 0.0f) {
      if (prevBatchSize_ != 0.0f) { 

        bool smallChange = false;
        if (not smallChange) {
          // TODO only change if difference is not due to jitter
          incrDirection_ = (currentBatchSize < prevBatchSize_) ? -1 : 1;
          const size_t minSubmitSz = ioexecPtr_->minSubmitSize() + incrDirection_;
          if ((minSubmitSz <= pt_->numConnections()) &&
            (minSubmitSz > 0)) {
            ioexecPtr_->setMinSubmitSize(minSubmitSz);
            minSubmitSizeStats_ = minSubmitSz;
          }
        }
      }
      prevBatchSize_ = currentBatchSize;
    }
    */

    // Dynamically vary minSubmitSize based on IOPS observed 
    // in previous second
    const auto currentOps = ioexecPtr_->stats_.read_.numOps_;
    if (currentOps != 0) {
      if (prevOps_ != 0) { 

        if (opsRecord_.empty()) {
          opsRecord_.reserve(60);
        }
        TimerPrint t{currentOps, 
          ioexecPtr_->minSubmitSize(),
          ioexecPtr_->stats_.numProcessedInLoop_.mean(),
          ioexecPtr_->stats_.numExternalFlushes_,
          ioexecPtr_->stats_.numInlineFlushes_,
          ioexecPtr_->stats_.numCompletionFlushes_};

        opsRecord_.push_back(std::move(t));

        int inversePercChange = 50; // 2 perc change in iops
        //bool smallChange = std::abs(currentOps - prevOps_) < (prevOps_/inversePercChange); // only change if difference is not due to jitter
        bool smallChange = false; // this works better
        if (not smallChange) {
          incrDirection_ = (currentOps < prevOps_) ? -1 : 1;
          const size_t minSubmitSz = ioexecPtr_->minSubmitSize() + incrDirection_;
          if (minSubmitSz > 0) {
            ioexecPtr_->setMinSubmitSize(minSubmitSz);
            minSubmitSizeStats_ = minSubmitSz;
          }
        }
      }
      prevOps_ = currentOps;
    }

    // no need to print if no connections exist
    if (++ timerCalled_ == 60) {
      GLOG_INFO("thread=" << gettid() 
        << ",portalId=" << pt_->coreId_ 
        << ",numConnections=" << pt_->numConnections()
        << ",current_minSubmitSize=" << ioexecPtr_->minSubmitSize()
        << ",stats_minSubmitSize=" << minSubmitSizeStats_
        << ",ioexec=" << ioexecPtr_->stats_.getState());

      timerCalled_ = 0;
      minSubmitSizeStats_.reset();

      std::ostringstream os;

      for (auto& op : opsRecord_) {
        os << ",p=" << pt_->coreId_ 
          << ",o=" << op.ops_ 
          << ",s=" << op.submitSize_ 
          << ",lp=" << op.processedInLoop_
          << ",ef=" << op.externalFlushes_ 
          << ",if=" << op.inlineFlushes_ 
          << ",cf=" << op.completionFlushes_ 
          << std::endl;
      }

      GLOG_INFO(os.str());

      opsRecord_.clear();
      opsRecord_.reserve(60);
    }
  }

  ioexecPtr_->stats_.clear();
}


int NetworkXioIOHandler::handle_multi_read(NetworkXioRequest *req,
                                     NetworkXioMsg& requestHeader) {

  req->op = NetworkXioMsgOpcode::ReadRsp;
  req->numElems_ = requestHeader.numElems_;
  req->clientMsgPtr_ = requestHeader.clientMsgPtr_;

  GLOG_DEBUG("ReadReq req=" << (void*)req << " numelem=" << req->numElems_);

  for (size_t idx = 0; idx < req->numElems_; idx ++) {
    int ret = handle_read(req, requestHeader.filenameVec_[idx], 
        requestHeader.sizeVec_[idx],
        requestHeader.offsetVec_[idx]);
    if (ret < 0) {
      req->completeElems_ ++;
    } else {
#ifdef BYPASS_READ
      req->completeElems_ ++;
#endif
    }
  }
  if (req->numElems_ == req->completeElems_) {
    // if all requests finished right now, lets pack em
    req->pack_msg();
    return -1;
  }
  return 0;
}

int NetworkXioIOHandler::handle_read(NetworkXioRequest *req,
                                     const std::string &filename, size_t size,
                                     off_t offset) {


  int ret = 0;
  req->op = NetworkXioMsgOpcode::ReadRsp;
#ifdef BYPASS_READ
  { 
    req->retvalVec_.push_back(size);
    req->errvalVec_.push_back(0);
    return 0;
  } 
#endif

  xio_reg_mem reg_mem;
  ret = xio_mempool_alloc(pt_->mpool_, size, &reg_mem);
  if (ret < 0) {
    // could not allocate from mempool, try mem alloc
    ret = xio_mem_alloc(size, &reg_mem);
    if (ret < 0) {
      GLOG_ERROR("cannot allocate requested buffer, size: " << size);
      req->retvalVec_.push_back(-1);
      req->errvalVec_.push_back(ENOMEM);
      return ret;
    }
    req->from_pool_vec.push_back(false);
    req->reg_mem_vec.push_back(reg_mem);
  } else {
    req->from_pool_vec.push_back(true);
    req->reg_mem_vec.push_back(reg_mem);
  }

  GLOG_DEBUG("Received read request for object "
     << " file=" << filename 
     << " at offset=" << offset 
     << " for size=" << size);

  try {

    bzero(static_cast<char *>(reg_mem.addr), size);

    gIOBatch *batch = gIOBatchAlloc(1);
    batch->opaque = req; // many batches point to one req
    gIOExecFragment &frag = batch->array[0];

    frag.offset = offset;
    frag.addr = reinterpret_cast<caddr_t>(reg_mem.addr);
    frag.size = size;
    frag.completionId = reinterpret_cast<uint64_t>(batch);

    // passing cd->coreId to the job allows the IOExecutor to execute
    // job on the same core as the accelio portal thread on which it
    // was received.  Not getting any perf improvement with this so
    // disabling it.
    ret = IOExecFileRead(serviceHandle_, filename.c_str(), filename.size(),
                         batch, 
                         eventFD_,
                         static_runEventHandler,
                         (void*)this);

    if (ret != 0) {
      GLOG_ERROR("IOExecFileRead failed with error " << ret << " req=" << (void*)req);
      req->retvalVec_.push_back(-1);
      req->errvalVec_.push_back(EIO);
      frag.addr = nullptr;
      gIOBatchFree(batch);
    }

    // CAUTION : only touch "req" after this if ret is non-zero
    // because "req" can get freed by the other thread if the IO completed
    // leading to a "use-after-free" memory error
  } catch (...) {
    GLOG_ERROR("failed to read volume ");
    req->retvalVec_.push_back(-1);
    req->errvalVec_.push_back(EIO);
  }

  return ret;
}

void NetworkXioIOHandler::handle_error(NetworkXioRequest *req, int errval) {
  req->op = NetworkXioMsgOpcode::ErrorRsp;
  const size_t numElems = req->numElems_;
  for (size_t idx = 0; idx < numElems; idx ++) {
    req->retvalVec_.push_back(-1);
    req->errvalVec_.push_back(errval);
  }
  req->pack_msg();
}

/*
 * @returns finishNow indicates whether response can be immediately sent to client 
 */
bool NetworkXioIOHandler::process_request(NetworkXioRequest *req) {
  bool finishNow = true;
  xio_msg *xio_req = req->xio_req;

  NetworkXioMsg requestHeader(NetworkXioMsgOpcode::Noop);
  try {
    requestHeader.unpack_msg(static_cast<char *>(xio_req->in.header.iov_base),
                     xio_req->in.header.iov_len);
  } catch (...) {
    GLOG_ERROR("cannot unpack message");
    handle_error(req, EBADMSG);
    return finishNow;
  }

  switch (requestHeader.opcode()) {
    case NetworkXioMsgOpcode::ReadReq: {
      GLOG_DEBUG(" Command ReadReq");
      auto ret = handle_multi_read(req, requestHeader);
      if (ret == 0) {
        finishNow = false;
      }
#ifdef BYPASS_READ
      finishNow = true;
#endif
      break;
    }
    default:
      GLOG_ERROR("Unknown command " << (int)requestHeader.opcode());
      handle_error(req, EIO);
  };
  return finishNow;
}

size_t NetworkXioIOHandler::numPendingRequests() {
  return ioexecPtr_->requestQueueSize();
}

void NetworkXioIOHandler::drainQueue() {
  ioexecPtr_->ProcessRequestQueue();
}

// this func runs in context of portal thread
void NetworkXioIOHandler::handle_request(NetworkXioRequest *req) {
  bool finishNow = process_request(req); 
  if (finishNow) {
    // this happens when all requests in batch failed
    pt_->server_->send_reply(req);
  }
  if (numPendingRequests()) { 
    // stop the loop so that we do custom handling
    // in top-level loop
    xio_context_stop_loop(pt_->ctx_);
  }
}
}
} // namespace gobjfs
