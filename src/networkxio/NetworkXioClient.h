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

extern void ovs_xio_aio_complete_request(void *request, ssize_t retval,
                                         int errval);

extern void ovs_xio_complete_request_control(void *request, ssize_t retval,
                                             int errval);

class NetworkXioClient {
public:
  NetworkXioClient(const std::string &uri, const uint64_t qd);

  ~NetworkXioClient();

  struct session_data {
    xio_context *ctx{nullptr};
    bool disconnected{false};
    bool disconnecting{false};
  };

  struct xio_msg_s {
    xio_msg xreq;
    const void *opaque{nullptr};
    NetworkXioMsg msg;
    std::string s_msg;
  };

  struct xio_ctl_s {
    xio_msg_s xmsg;
    session_data sdata;
    std::vector<std::string> *vec;
    uint64_t size{0};
  };

  void xio_send_open_request(const void *opaque);

  void xio_send_close_request(const void *opaque);

  void xio_send_read_request(const std::string &filename, void *buf,
                             const uint64_t size_in_bytes,
                             const uint64_t offset_in_bytes,
                             const void *opaque);

  int allocate(xio_reg_mem *mem, const uint64_t size);

  void deallocate(xio_reg_mem *reg_mem);

  int on_session_event(xio_session *session,
                       xio_session_event_data *event_data);

  int on_response(xio_session *session, xio_msg *reply, int last_in_rxq);

  int on_msg_error(xio_session *session, xio_status error,
                   xio_msg_direction direction, xio_msg *msg);

  void evfd_stop_loop(int fd, int events, void *data);

  void run(std::promise<bool> &promise);

  bool is_queue_empty();

  xio_msg_s *pop_request();

  void push_request(xio_msg_s *req);

  void xstop_loop();

  static void xio_destroy_ctx_shutdown(xio_context *ctx);

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

private:
  std::shared_ptr<xio_context> ctx;
  std::shared_ptr<xio_session> session;
  xio_connection *conn{nullptr};
  xio_session_params params;
  xio_connection_params cparams;

  std::string uri_;
  bool stopping{false};
  bool stopped{false};
  std::thread xio_thread_;

  gobjfs::os::Spinlock inflight_lock;

  std::queue<xio_msg_s *> inflight_reqs;

  xio_session_ops ses_ops;
  bool disconnected{false};
  bool disconnecting{false};

  int64_t nr_req_queue{0};
  std::mutex req_queue_lock;
  std::condition_variable req_queue_cond;

  EventFD evfd;

  std::shared_ptr<xio_mempool> mpool;

  void xio_run_loop_worker(void *arg);

  void req_queue_wait_until(xio_msg_s *xmsg);

  void req_queue_release();

  void shutdown();

  template <class E> void set_exception_ptr(E e);

  static xio_connection *create_connection_control(session_data *sdata,
                                                   const std::string &uri);

  static int on_msg_control(xio_session *session, xio_msg *reply,
                            int last_in_rqx, void *cb_user_context);

  static int on_msg_error_control(xio_session *session, xio_status error,
                                  xio_msg_direction direction, xio_msg *msg,
                                  void *cb_user_context);

  static int on_session_event_control(xio_session *session,
                                      xio_session_event_data *event_data,
                                      void *cb_user_context);

  static void xio_msg_prepare(xio_msg_s *xmsg);

};

typedef std::shared_ptr<NetworkXioClient> NetworkXioClientPtr;
}
} // namespace
