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

#include "NetworkXioMsg.h"

#include "NetworkXioWorkQueue.h"
#include "NetworkXioRequest.h"

#include <util/ShutdownNotifier.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <assert.h>

#include "gIOExecFile.h"

namespace gobjfs { namespace xio
{

class NetworkXioIOHandler
{
public:
    NetworkXioIOHandler(const std::string& configFileName, NetworkXioWorkQueuePtr wq)
    : configFileName_(configFileName)
    , wq_(wq) {}

    ~NetworkXioIOHandler();

    NetworkXioIOHandler(const NetworkXioIOHandler&) = delete;

    NetworkXioIOHandler&
    operator=(const NetworkXioIOHandler&) = delete;

    void
    process_request(NetworkXioRequest *req);

    void
    handle_request(NetworkXioRequest* req);

    int 
    gxio_completion_handler(int epollfd, int efd);

private:
    void handle_open(NetworkXioRequest *req);

    void handle_close(NetworkXioRequest *req);

    void handle_read(NetworkXioRequest *req,
                     const std::string& filename,
                     size_t size,
                     off_t offset);

    void handle_error(NetworkXioRequest *req,
                      int errval);

private:

    NetworkXioWorkQueuePtr wq_;
    std::string configFileName_;

    IOExecServiceHandle    serviceHandle_{nullptr};
    IOExecEventFdHandle    eventHandle_{nullptr};

    int epollfd = -1 ; 

    std::thread ioCompletionThread;
    gobjfs::os::ShutdownNotifier ioCompletionThreadShutdown;
};

typedef std::unique_ptr<NetworkXioIOHandler> NetworkXioIOHandlerPtr;

}} //namespace 


