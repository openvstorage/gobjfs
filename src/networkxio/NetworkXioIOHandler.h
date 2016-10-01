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

#include "NetworkXioWorkQueue.h"
#include "NetworkXioRequest.h"

#include <util/ShutdownNotifier.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <assert.h>

#include "gIOExecFile.h"

namespace gobjfs {
namespace xio {

class NetworkXioIOHandler {
public:
  NetworkXioIOHandler(IOExecServiceHandle serviceHandle, 
      IOExecEventFdHandle eventHandle,
      NetworkXioWorkQueuePtr wq);

  ~NetworkXioIOHandler();

  NetworkXioIOHandler(const NetworkXioIOHandler &) = delete;

  NetworkXioIOHandler &operator=(const NetworkXioIOHandler &) = delete;

  // @return whether req is finished into workQueue on func return
  bool process_request(NetworkXioRequest *req);

  void handle_request(NetworkXioRequest *req);

private:
  void handle_open(NetworkXioRequest *req);

  int handle_read(NetworkXioRequest *req, const std::string &filename,
                  size_t size, off_t offset);

  void handle_error(NetworkXioRequest *req, int errval);

private:
  std::string configFileName_;

  IOExecServiceHandle serviceHandle_{nullptr};

  IOExecEventFdHandle eventHandle_{nullptr};

  NetworkXioWorkQueuePtr wq_;

};

typedef std::unique_ptr<NetworkXioIOHandler> NetworkXioIOHandlerPtr;
}
} // namespace
