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
#include <atomic>
#include <queue>
#include <thread>
#include <boost/thread/lock_guard.hpp>
#include <libxio.h>
#include <gobjfs_log.h>

#include <networkxio/gobjfs_client_common.h>
#include <networkxio/NetworkXioCommon.h>
#include <util/Spinlock.h>
#include <util/EventFD.h>

using gobjfs::os::EventFD;

namespace gobjfs {
namespace xio {

class NetworkXioServer;
class NetworkXioIOHandler;

struct NetworkXioClientData;

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
      NetworkXioClientData* clientData,
      NetworkXioServer* server)
    : xio_req(req)
    , pClientData(clientData)
  {
  }
};


struct NetworkXioClientData {

  NetworkXioServer *ncd_server{nullptr};

  // TODO make them shared_ptr like NetworkXioServer
  xio_context *ncd_ctx{nullptr}; // portal
  std::thread ncd_thread; // portal
  std::string ncd_uri; // portal
  xio_server* ncd_xio_server{nullptr}; // portal

  xio_session *ncd_session{nullptr};
  xio_connection *ncd_conn{nullptr};
  xio_mempool *ncd_mpool{nullptr};

  std::atomic<bool> ncd_disconnected{true};
  std::atomic<uint64_t> ncd_refcnt{0};

  NetworkXioIOHandler *ncd_ioh{nullptr};

  std::list<NetworkXioRequest *> ncd_done_reqs;

  int coreId_{-1};

  EventFD evfd;

  void evfd_stop_loop(int /*fd*/, int /*events*/, void * /*data*/) {
    evfd.readfd();
    xio_context_stop_loop(ncd_ctx);
  }

  void stop_loop() {
    evfd.writefd();
  }

  explicit NetworkXioClientData(NetworkXioServer* server,
      const std::string& uri,
      int coreId)
    : ncd_server(server)
    , ncd_uri(uri) 
    , coreId_(coreId) {}
};

}
} // namespace
