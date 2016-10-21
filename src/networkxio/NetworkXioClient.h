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
#include <vector>
#include <future>
#include <mutex>
#include <boost/thread/lock_guard.hpp>
#include <assert.h>
#include <thread>
#include <atomic>
#include <exception>
#include <util/Spinlock.h>
#include <util/Stats.h>
#include "NetworkXioProtocol.h"

namespace gobjfs {
namespace xio {

struct aio_request;

MAKE_EXCEPTION(XioClientCreateException);
MAKE_EXCEPTION(XioClientRegHandlerException);
MAKE_EXCEPTION(XioClientQueueIsBusyException);
MAKE_EXCEPTION(FailedRegisterEventHandler);


/**
 * One NetworkXioClient can only connect to single URI (i.e. NetworkXioServer)
 * And xio_context <-> xio_connection is 1:1
 * Only one thread should access one xio_context at a time
 *
 * To connect to multiple portals on same URI, 
 * OR To connect to multiple URIs
 * call ctx_init(uri) multiple times
 */
class NetworkXioClient {
public:
  NetworkXioClient(const std::string &uri, const uint64_t qd);

  ~NetworkXioClient();

  struct ClientMsg {

    // header message actually sent to server
    xio_msg xreq;           

    // points to list of original aio_requests
    // which comprise this single server message
    std::vector<void*> aioReqVec_;
    
    // arguments to be sent to server in header
    NetworkXioMsg msg;

    // msgpack buffer generated from NetworkXioMsg
    // which is sent as header in xio_msg
    std::string msgpackBuffer;

    ClientMsg();

    void prepare();
  };

  int send_multi_read_request(std::vector<std::string> &&filenameVec, 
    std::vector<void *>   &&bufVec,
    std::vector<uint64_t> &&sizeVec,
    std::vector<uint64_t> &&offsetVec,
    const std::vector<void *>   &aioReqVec);

  int send_read_request(const std::string &filename, void *buf,
                             const uint64_t size_in_bytes,
                             const uint64_t offset_in_bytes,
                             void *opaque);

  int on_session_event(xio_session *session,
                       xio_session_event_data *event_data);

  int on_response(xio_session *session, xio_msg *reply, int last_in_rxq);

  int on_msg_error(xio_session *session, xio_status error,
                   xio_msg_direction direction, xio_msg *msg);

  void run();

  struct statistics {

    std::atomic<uint64_t> num_queued{0};
    // num_completed includes num_failed
    std::atomic<uint64_t> num_completed{0};
    std::atomic<uint64_t> num_failed{0};

    gobjfs::stats::Histogram<int64_t> rtt_hist;
    gobjfs::stats::StatsCounter<int64_t> rtt_stats;

    std::string ToString() const;

  } stats;

  void update_stats(void *req, bool req_failed);

  const bool &is_disconnected() { return disconnected; }

  void send_msg(ClientMsg *msgPtr);

  void run_loop();

private:
  std::shared_ptr<xio_context> ctx;
  std::shared_ptr<xio_session> session;
  xio_connection *conn{nullptr};
  xio_session_params sparams;
  xio_connection_params cparams;

  std::string uri_;
  bool stopping{false};
  bool stopped{false};

public:
  size_t maxBatchSize_ = 4; // TODO dynamic

private:
  xio_session_ops ses_ops;
  bool disconnected{false};
  bool disconnecting{false};

  int64_t nr_req_queue{0};

  void xio_run_loop_worker(void *arg);

  void shutdown();

};

typedef std::shared_ptr<NetworkXioClient> NetworkXioClientPtr;
}
} // namespace
