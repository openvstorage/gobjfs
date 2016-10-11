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
#pragma once

#include "NetworkXioClient.h"

namespace gobjfs {
namespace xio {

struct client_ctx {
  TransportType transport;
  std::string host;
  int port{-1};
  std::string uri;
  gobjfs::xio::NetworkXioClientPtr net_client_;

  ~client_ctx() { net_client_.reset(); }
};

inline aio_request *create_new_request(RequestOp op, struct giocb *aio,
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

}
}
