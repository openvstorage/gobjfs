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

#ifndef __NETWORK_XIO_IO_HANDLER_H_
#define __NETWORK_XIO_IO_HANDLER_H_

//#include "FileSystem.h"

#include "NetworkXioWorkQueue.h"
#include "NetworkXioRequest.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <assert.h>

#include "gIOExecFile.h"

namespace volumedriverfs
{

class NetworkXioIOHandler
{
public:
    /*NetworkXioIOHandler(FileSystem& fs,
                        NetworkXioWorkQueuePtr wq)*/
    NetworkXioIOHandler(NetworkXioWorkQueuePtr wq)
    : wq_(wq) {
        dev_name_ = "";
     }

    ~NetworkXioIOHandler()
    {
        if (serviceHandle_ )
        {
          // TODO 
          IOExecFileServiceDestroy(serviceHandle_);
          serviceHandle_ = NULL;
        }
        dev_name_ = "";

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
    void handle_open(NetworkXioRequest *req,
                     const std::string& dev_name);

    void handle_close(NetworkXioRequest *req);

    void handle_read(NetworkXioRequest *req,
                     uint64_t gobjid_,
                     size_t size,
                     uint64_t offset);

    void handle_flush(NetworkXioRequest *req);

    void handle_error(NetworkXioRequest *req,
                      int errval);

private:
   // DECLARE_LOGGER("NetworkXioIOHandler");

    //FileSystem& fs_;
    NetworkXioWorkQueuePtr wq_;

    std::string dev_name_;
    IOExecServiceHandle    serviceHandle_{nullptr};
    IOExecEventFdHandle    eventHandle_{nullptr};
    int epollfd = -1 ; 
    std::thread *pioCompletionThread;

    std::string
    make_volume_path(const std::string& dev_name)
    {
        const std::string root_("/");
        //return (root_ + dev_name + fs_.vdisk_format().volume_suffix());
        return (root_ + dev_name);
    }
};

typedef std::unique_ptr<NetworkXioIOHandler> NetworkXioIOHandlerPtr;

} //namespace volumedriverfs


#endif
