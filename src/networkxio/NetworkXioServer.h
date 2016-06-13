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

#include <map>
#include <tuple>
#include <memory>
#include <libxio.h>
#include <iostream>
#include <atomic>
#include "NetworkXioIOHandler.h"
#include "NetworkXioWorkQueue.h"
#include "NetworkXioRequest.h"

namespace gobjfs { namespace xio 
{

class NetworkXioClientData;

class NetworkXioServer
{
public:

    NetworkXioServer(const std::string& uri,
      const std::string& configFileName);

    ~NetworkXioServer();

    NetworkXioServer(const NetworkXioServer&) = delete;

    NetworkXioServer&
    operator=(const NetworkXioServer&) = delete;

    int
    on_request(xio_session *session,
               xio_msg *req,
               int last_in_rxq,
               void *cb_user_context);

    int
    on_session_event(xio_session *session,
                     xio_session_event_data *event_data);

    int
    on_new_session(xio_session *session,
                   xio_new_session_req *req);

    int
    on_msg_send_complete(xio_session *session,
                         xio_msg *msg,
                         void *cb_user_context);

    int
    on_msg_error(xio_session *session,
                 xio_status error,
                 xio_msg_direction direction,
                 xio_msg *msg);

    int
    assign_data_in_buf(xio_msg *msg);

    void
    run();

    void
    shutdown();

    void
    xio_send_reply(NetworkXioRequest *req);

    void
    evfd_stop_loop(int fd, int events, void *data);
private:
//    DECLARE_LOGGER("NetworkXioServer");

    std::string uri_;
    std::string configFileName_; 

    bool stopping{false};

    std::mutex mutex_;
    std::condition_variable cv_;
    bool stopped{false};

    NetworkXioWorkQueuePtr wq_;

    xio_context *ctx{nullptr};
    xio_server *server{nullptr};
    xio_mempool *xio_mpool{nullptr};
    int evfd{-1};

    int
    create_session_connection(xio_session *session,
                              xio_session_event_data *event_data);

    void
    destroy_session_connection(xio_session *session,
                               xio_session_event_data *event_data);

    NetworkXioRequest*
    allocate_request(NetworkXioClientData *cd, xio_msg *xio_req);

    void
    deallocate_request(NetworkXioRequest *req);

    void
    free_request(NetworkXioRequest *req);
};

}} //namespace

