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
#include <boost/lockfree/queue.hpp>
#include <future>
#include <mutex>
#include <boost/thread/lock_guard.hpp>
#include <assert.h>
#include <thread>
#include <atomic>
#include <exception>
#include <util/Spinlock.h>
#include <util/Stats.h>
#include <util/EventFD.h>
#include <util/TimerNotifier.h>
#include "NetworkXioProtocol.h"
#include "gobjfs_config.h"

namespace gobjfs {
namespace xio {

struct aio_request;

MAKE_EXCEPTION(XioClientCreateException);
MAKE_EXCEPTION(XioClientRegHandlerException);
MAKE_EXCEPTION(XioClientQueueIsBusyException);
MAKE_EXCEPTION(FailedRegisterEventHandler);

typedef std::shared_ptr<xio_session> SessionPtr;

class NetworkXioClient {
public:
  NetworkXioClient(const uint64_t qd);
  
  ~NetworkXioClient();

  struct ClientMsg {

    // which connection was used
    int32_t uriSlot;

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

    ClientMsg(int32_t uri_slot) : uriSlot(uri_slot) {}

    void prepare();
  };

  int append_read_request(const std::string &filename, void *buf,
                             const uint64_t size_in_bytes,
                             const uint64_t offset_in_bytes,
                             void *opaque,
                             int32_t uri_slot = 0);

  void send_msg(ClientMsg *msgHeader);

  int on_session_event(xio_session *session,
                       xio_session_event_data *event_data);

  int on_response(xio_session *session, xio_msg *reply, int last_in_rxq);

  int on_msg_error(xio_session *session, xio_status error,
                   xio_msg_direction direction, xio_msg *msg);

  void run(std::promise<bool>& promise);

  int connect(const std::string& uri);

  struct statistics {

    // queue length seen by timer
    gobjfs::stats::StatsCounter<int64_t> timeout_queue_len;
    // queue length seen during direct sends
    gobjfs::stats::StatsCounter<int64_t> direct_queue_len;

    std::atomic<uint64_t> num_queued{0};
    // num_completed includes num_failed
    std::atomic<uint64_t> num_completed{0};
    std::atomic<uint64_t> num_failed{0};

    gobjfs::stats::Histogram<int64_t> rtt_hist;
    gobjfs::stats::StatsCounter<int64_t> rtt_stats;

    std::string ToString() const;

  } stats;

  void update_stats(void *req, bool req_failed);

  bool is_disconnected(int32_t uri_slot);


private:
  std::shared_ptr<xio_context> ctx;

  std::vector<SessionPtr> sessionVec;
  std::vector<xio_connection*> connVec;
  std::vector<std::string> uriVec;

  std::mutex currentMsgMutex;
  ClientMsg *currentMsgPtr = nullptr;

  xio_session_params params;
  xio_connection_params cparams;

  bool stopping{false};
  bool stopped{false};
  std::thread xio_thread_;

public:

  int timeout_ms = XIO_INFINITE; 

  size_t maxBatchSize_ = MAX_AIO_BATCH_SIZE;
private:

  std::atomic<int32_t> numQueued_{0};
  const int32_t maxQueued_{0};
  boost::lockfree::queue<ClientMsg*, boost::lockfree::fixed_sized<true>> inflight_reqs;

  std::atomic<int32_t> waiters_{0};
  std::mutex inflight_mutex;
  std::condition_variable inflight_cond;

public:
  int32_t inflight_queue_size();
  ClientMsg* pop_request();
  void push_request(ClientMsg* req);

  ClientMsg* allocClientMsg(int32_t uri_slot);
  int flush(ClientMsg* sendMsgPtr);

private:

  xio_session_ops ses_ops;

  EventFD evfd;
public:
  void evfd_stop_loop(int fd, int events, void* data);
  void xstop_loop();

private:

  void run_loop_worker();

  void shutdown();

  std::unique_ptr<gobjfs::os::TimerNotifier> flushTimerFD_;

public:
  void runTimerHandler();

};

typedef std::shared_ptr<NetworkXioClient> NetworkXioClientPtr;
}
} // namespace
