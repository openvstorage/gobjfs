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
 * one per request
 */
struct NetworkXioRequest {
  NetworkXioMsgOpcode op{NetworkXioMsgOpcode::Noop};

  void *data{nullptr};
  unsigned int data_len{0}; // DataLen of buffer pointed by data
  size_t size{0};           // Size to be written/read.
  uint64_t offset{0};       // at which offset

  ssize_t retval{0};
  int errval{0};
  uintptr_t opaque{0};

  xio_msg *xio_req{nullptr};
  xio_msg xio_reply;
  xio_reg_mem reg_mem;
  bool from_pool{false};

  NetworkXioClientData *pClientData{nullptr};

  void *private_data{nullptr};

  std::string s_msg;

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
