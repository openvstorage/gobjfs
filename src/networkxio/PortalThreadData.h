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

/**
 * This object is one per Portal thread created by the server
 * Array of these is maintained by NetworkXioServer
 * it contains NetworkXioIOHandler which contains handlers for various functions
 */
struct PortalThreadData {

  NetworkXioServer* server_{nullptr}; 
  NetworkXioIOHandler* ioh_{nullptr};
  
  xio_server* xio_server_{nullptr};

  std::string uri_; 
  std::thread thread_; 

  // context must be per-thread as per accelio
  xio_context *ctx_{nullptr};  

  xio_mempool *mpool_{nullptr};

  EventFD evfd_;

  // number of connections currently handled by this thread
  size_t numConnections_{0};

  bool stopping = false;
  bool stopped = false;

  // one which core is the thread bound
  int coreId_{-1};

  // to stop the thread event loop, call this func
  void stop_loop();

  // called from accelio event loop only
  void evfd_stop_loop(int /*fd*/, int /*events*/, void * /*data*/);

  PortalThreadData(NetworkXioServer* server, const std::string& uri, int coreId)
    : server_(server)
    , uri_(uri)
    , coreId_(coreId) 
    {}

  ~PortalThreadData();

  private:

  // the thread executes this func
  void portal_func();

  friend class NetworkXioServer;
};

}
}
