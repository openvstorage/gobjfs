// Copyright 2016 iNuron NV
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

//#include "FileSystem.h"

#include "NetworkXioMsg.h"

#include "NetworkXioWorkQueue.h"
#include "NetworkXioRequest.h"

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
    /*NetworkXioIOHandler(FileSystem& fs,
                        NetworkXioWorkQueuePtr wq)*/
    NetworkXioIOHandler(const std::string& configFileName, NetworkXioWorkQueuePtr wq)
    : configFileName_(configFileName)
    , wq_(wq) {}

    ~NetworkXioIOHandler()
    {
        // TODO signal exit
        ioCompletionThread.join();

        if (serviceHandle_ )
        {
          IOExecFileServiceDestroy(serviceHandle_);
          serviceHandle_ = NULL;
        }
    }

    NetworkXioIOHandler(const NetworkXioIOHandler&) = delete;

    NetworkXioIOHandler&
    operator=(const NetworkXioIOHandler&) = delete;

    void
    process_request(NetworkXioRequest *req);

    void
    handle_request(NetworkXioRequest* req);

    std::thread CompletionThread;
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
   // DECLARE_LOGGER("NetworkXioIOHandler");

    NetworkXioWorkQueuePtr wq_;
    std::string configFileName_;

    IOExecServiceHandle    serviceHandle_{nullptr};
    IOExecEventFdHandle    eventHandle_{nullptr};
    int epollfd = -1 ; 
    std::thread ioCompletionThread;
};

typedef std::unique_ptr<NetworkXioIOHandler> NetworkXioIOHandlerPtr;

}} //namespace 


