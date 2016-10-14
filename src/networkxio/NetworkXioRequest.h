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

#include <list>
#include <functional>
#include <queue>
#include <thread>
#include <boost/thread/lock_guard.hpp>
#include <libxio.h>
#include <gobjfs_log.h>

#include <networkxio/gobjfs_client_common.h>
#include <networkxio/NetworkXioCommon.h>
#include <util/Spinlock.h>
#include <util/EventFD.h>


namespace gobjfs {
namespace xio {

class NetworkXioServer;
class NetworkXioIOHandler;
class PortalThreadData;
struct NetworkXioClientData;

/**
 * allocated one per request on server-side
 */
struct NetworkXioRequest {
  NetworkXioMsgOpcode op{NetworkXioMsgOpcode::Noop};

  std::vector<ssize_t> retvalVec_;
  std::vector<int> errvalVec_;

  uintptr_t headerPtr_; // header allocated on client side 
  size_t numElems_{0};
  size_t completeElems_{0};

  xio_msg *xio_req{nullptr}; // message received from client
  xio_msg xio_reply;         // structure send back to client

  std::vector<xio_reg_mem> reg_mem_vec;       // memory allocated from xio
  std::vector<bool> from_pool_vec;

  NetworkXioClientData *pClientData{nullptr};

  void *private_data{nullptr};

  std::string msgpackBuffer;         // msgpack buffer which is sent back as header

  void pack_msg();

  explicit NetworkXioRequest(xio_msg* req,
      NetworkXioClientData* clientData)
    : xio_req(req)
    , pClientData(clientData)
  {
  }
};


/**
 * created on new connection event 
 * it is one per connection
 * destroyed when connection is closed
 */
struct NetworkXioClientData {

  PortalThreadData* pt_;
  NetworkXioServer *server_{nullptr};

  xio_session *ncd_session{nullptr};
  xio_connection *ncd_conn{nullptr};

  // TODO make this enum
  int conn_state = 0; // 1 = conn, 2 = disconn

  uint64_t ncd_refcnt{0};

  std::list<NetworkXioRequest *> ncd_done_reqs;

  explicit NetworkXioClientData(PortalThreadData* pt,
      xio_session* session,
      xio_connection* conn)
    : pt_(pt)
    , ncd_session(session)
    , ncd_conn(conn) 
    , conn_state(1)
  {
  }
};

}
} // namespace
