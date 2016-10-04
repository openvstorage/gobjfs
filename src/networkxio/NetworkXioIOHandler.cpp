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

#include "NetworkXioIOHandler.h"
#include <networkxio/NetworkXioCommon.h>
#include "NetworkXioProtocol.h"
#include "NetworkXioServer.h"

using namespace std;
namespace gobjfs {
namespace xio {


static inline void pack_msg(NetworkXioRequest *req) {
  NetworkXioMsg o_msg(req->op);
  o_msg.retval(req->retval);
  o_msg.errval(req->errval);
  o_msg.opaque(req->opaque);
  req->s_msg = o_msg.pack_msg();
}

NetworkXioIOHandler::NetworkXioIOHandler(NetworkXioServer* server)
    : server_(server) {}

NetworkXioIOHandler::~NetworkXioIOHandler() {
}

void NetworkXioIOHandler::handle_open(NetworkXioRequest *req) {
  int err = 0;
  GLOG_DEBUG("trying to open volume ");

  req->op = NetworkXioMsgOpcode::OpenRsp;
  req->retval = 0;
  req->errval = 0;

  pack_msg(req);
}

int NetworkXioIOHandler::handle_read(NetworkXioRequest *req,
                                     const std::string &filename, size_t size,
                                     off_t offset) {
  int ret = 0;
  req->op = NetworkXioMsgOpcode::ReadRsp;
#ifdef BYPASS_READ
  { 
    req->retval = size;
    req->errval = 0;
    pack_msg(req);
    return 0;
  } 
#endif
  if (!server_->serviceHandle_) {
    GLOG_ERROR("no service handle");
    req->retval = -1;
    req->errval = EIO;
    pack_msg(req);
    return -1;
  }

  ret = xio_mempool_alloc(req->pClientData->ncd_mpool, size, &req->reg_mem);
  if (ret < 0) {
    // could not allocate from mempool, try mem alloc
    ret = xio_mem_alloc(size, &req->reg_mem);
    if (ret < 0) {
      GLOG_ERROR("cannot allocate requested buffer, size: " << size);
      req->retval = -1;
      req->errval = ENOMEM;
      pack_msg(req);
      return ret;
    }
    req->from_pool = false;
  } else {
    req->from_pool = true;
  }

  GLOG_DEBUG("Received read request for object "
     << " file=" << filename 
     << " at offset=" << offset 
     << " for size=" << size);

  req->data = req->reg_mem.addr;
  req->data_len = size;
  req->size = size;
  req->offset = offset;
  try {

    memset(static_cast<char *>(req->data), 0, req->size);

    gIOBatch *batch = gIOBatchAlloc(1);
    batch->opaque = req;
    gIOExecFragment &frag = batch->array[0];

    frag.offset = offset;
    frag.addr = reinterpret_cast<caddr_t>(req->reg_mem.addr);
    frag.size = size;
    frag.completionId = reinterpret_cast<uint64_t>(batch);

    ret = IOExecFileRead(server_->serviceHandle_, filename.c_str(), filename.size(),
                         batch, server_->disk_.eventHandle_);

    if (ret != 0) {
      GLOG_ERROR("IOExecFileRead failed with error " << ret);
      req->retval = -1;
      req->errval = EIO;
      frag.addr = nullptr;
      gIOBatchFree(batch);
    }

    // CAUTION : only touch "req" after this if ret is non-zero
    // because "req" can get freed by the other thread if the IO completed
    // leading to a "use-after-free" memory error
  } catch (...) {
    GLOG_ERROR("failed to read volume ");
    req->retval = -1;
    req->errval = EIO;
  }

  if (ret != 0) {
    pack_msg(req);
  }
  return ret;
}

void NetworkXioIOHandler::handle_error(NetworkXioRequest *req, int errval) {
  req->op = NetworkXioMsgOpcode::ErrorRsp;
  req->retval = -1;
  req->errval = errval;
  pack_msg(req);
}

/*
 * @returns finishNow indicates whether response can be immediately sent to client 
 */
bool NetworkXioIOHandler::process_request(NetworkXioRequest *req) {
  bool finishNow = true;
  xio_msg *xio_req = req->xio_req;
  xio_iovec_ex *isglist = vmsg_sglist(&xio_req->in);
  int inents = vmsg_sglist_nents(&xio_req->in);

  NetworkXioMsg i_msg(NetworkXioMsgOpcode::Noop);
  try {
    i_msg.unpack_msg(static_cast<char *>(xio_req->in.header.iov_base),
                     xio_req->in.header.iov_len);
  } catch (...) {
    GLOG_ERROR("cannot unpack message");
    handle_error(req, EBADMSG);
    return finishNow;
  }

  req->opaque = i_msg.opaque();
  switch (i_msg.opcode()) {
    case NetworkXioMsgOpcode::OpenReq: {
      GLOG_DEBUG(" Command OpenReq");
      handle_open(req);
      break;
    }
    case NetworkXioMsgOpcode::ReadReq: {
      GLOG_DEBUG(" Command ReadReq");
      auto ret = handle_read(req, i_msg.filename_, i_msg.size(), i_msg.offset());
      if (ret == 0) {
        finishNow = false;
      }
#ifdef BYPASS_READ
      finishNow = true;
#endif
      break;
    }
    default:
      GLOG_ERROR("Unknown command " << (int)i_msg.opcode());
      handle_error(req, EIO);
  };
  return finishNow;
}

void NetworkXioIOHandler::handle_request(NetworkXioRequest *req) {
  auto ret = process_request(req); 
  if (ret == true) {
    // this means request has error and can be immediately  
    // sent back to client
    req->pClientData->worker_bottom_half(req);
  }
}
}
} // namespace gobjfs
