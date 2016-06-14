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
#include <iostream>
#include <queue>
#include <boost/thread/lock_guard.hpp>
#include <assert.h>
#include <thread>
#include <atomic>

#include "NetworkXioMsg.h"

namespace gobjfs { namespace xio 
{

extern void ovs_xio_aio_complete_request(void *request,
                                         ssize_t retval,
                                         int errval);

extern void ovs_xio_complete_request_control(void *request,
                                             ssize_t retval,
                                             int errval);

class NetworkXioClient
{
public:
    NetworkXioClient(const std::string& uri);

    ~NetworkXioClient();

    struct xio_msg_s
    {
        xio_msg xreq;
        const void *opaque{nullptr};
        NetworkXioMsg msg;
        std::string s_msg;
    };

    struct xio_ctl_s
    {
        xio_msg_s xmsg;
        std::vector<std::string> *vec;
        uint64_t size;
    };

    void
    xio_send_open_request(const void *opaque);

    void
    xio_send_close_request(const void *opaque);

    void
    xio_send_read_request(const std::string& filename,
                        void *buf,
                        const uint64_t size_in_bytes,
                        const uint64_t offset_in_bytes,
                        const void *opaque);

    int
    on_session_event(xio_session *session,
                     xio_session_event_data *event_data);

    int
    on_response(xio_session *session,
                xio_msg* reply,
                int last_in_rxq);

    int
    on_msg_error(xio_session *session,
                 xio_status error,
                 xio_msg_direction direction,
                 xio_msg *msg);

    void
    evfd_stop_loop(int fd, int events, void *data);

    bool
    is_queue_empty();

    xio_msg_s*
    pop_request();

    void
    push_request(xio_msg_s *req);

    void
    xstop_loop();

private:
    xio_context *ctx{nullptr};
    xio_session *session{nullptr};
    xio_connection *conn{nullptr};
    xio_session_params params;
    xio_connection_params cparams;

    std::string uri_;
    bool stopping{false};
    std::thread xio_thread_;

    std::atomic_flag  inflight_lock = ATOMIC_FLAG_INIT;

    inline void lock_ts() {
        while (inflight_lock.test_and_set(std::memory_order_acquire));
    }
    
    inline void unlock_ts() {
        inflight_lock.clear(std::memory_order_release);
    }

    std::queue<xio_msg_s*> inflight_reqs;

    xio_session_ops ses_ops;
    bool disconnected{false};

    int evfd{-1};

    void
    xio_run_loop_worker(void *arg);

    static xio_connection*
    create_connection_control(xio_context *ctx,
                              const std::string& uri);

    static int
    on_msg_control(xio_session *session,
                   xio_msg *reply,
                   int last_in_rqx,
                   void *cb_user_context);

    static int
    on_msg_error_control(xio_session *session,
                         xio_status error,
                         xio_msg_direction direction,
                         xio_msg *msg,
                         void *cb_user_context);

    static int
    on_session_event_control(xio_session *session,
                             xio_session_event_data *event_data,
                             void *cb_user_context);

    static void
    xio_submit_request(const std::string& uri,
                       xio_ctl_s *xctl,
                       void *opaque);

    static void
    xio_msg_prepare(xio_msg_s *xmsg);

};

typedef std::shared_ptr<NetworkXioClient> NetworkXioClientPtr;

}} //namespace 

