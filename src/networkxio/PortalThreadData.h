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

#include <libxio.h>
#include <string>
#include <thread>
#include <util/EventFD.h>

namespace gobjfs {
namespace xio {

class NetworkXioServer;
class NetworkXioIOHandler;

struct PortalThreadData {

  NetworkXioServer* server_{nullptr}; 
  NetworkXioIOHandler* ioh_{nullptr};
  
  xio_server* xio_server_{nullptr};

  std::string uri_; 
  std::thread thread_; 

  xio_context *ctx_{nullptr};  
  xio_mempool *mpool_{nullptr};
  EventFD evfd_;
  size_t numConnections_{0};

  bool stopping = false;
  bool stopped = false;

  int coreId_{-1};

  void evfd_stop_loop(int /*fd*/, int /*events*/, void * /*data*/);

  void stop_loop();

  void portal_func();

  PortalThreadData(NetworkXioServer* server, const std::string& uri, int coreId)
    : server_(server)
    , uri_(uri)
    , coreId_(coreId) 
    {}

  ~PortalThreadData() {
    delete ioh_;
    // TODO free others
  }
};

}
}
