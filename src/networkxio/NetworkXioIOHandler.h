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

#include "NetworkXioRequest.h"
#include <gcommon.h>

#include <util/ShutdownNotifier.h>
#include <util/Pipe.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <assert.h>
#include <list>

#include "gIOExecFile.h"

namespace gobjfs {
namespace xio {

class NetworkXioIOHandler {
public:
  NetworkXioIOHandler(PortalThreadData* pt);

  ~NetworkXioIOHandler();

  NetworkXioIOHandler(const NetworkXioIOHandler &) = delete;

  NetworkXioIOHandler &operator=(const NetworkXioIOHandler &) = delete;

  // @return whether req is finished 
  bool process_request(NetworkXioRequest *req);

  void handle_request(NetworkXioRequest *req);

  void drainQueue();

  int runEventHandler(gIOStatus& iostatus);

  std::list<NetworkXioRequest*> workQueue;
  bool firstCall = true;

private:
  void handle_open(NetworkXioRequest *req);

  int handle_read(NetworkXioRequest *req, const std::string &filename,
                  size_t size, off_t offset);

  void handle_error(NetworkXioRequest *req, int errval);

private:
  std::string configFileName_;

  IOExecServiceHandle serviceHandle_{nullptr};

  IOExecEventFdHandle eventHandle_{nullptr};

  int eventFD_{-1};

  PortalThreadData* pt_;

  void startEventHandler();
  void stopEventHandler();

};

typedef std::unique_ptr<NetworkXioIOHandler> NetworkXioIOHandlerPtr;
}
} // namespace
