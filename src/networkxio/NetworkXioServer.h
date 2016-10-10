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
#include <future>
#include <tuple>
#include <memory>
#include <libxio.h>
#include <iostream>
#include <atomic>
#include <networkxio/NetworkXioIOHandler.h>
#include <networkxio/NetworkXioRequest.h>
#include <networkxio/gobjfs_config.h>
#include <util/EventFD.h>

namespace gobjfs {
namespace xio {

MAKE_EXCEPTION(FailedBindXioServer);
MAKE_EXCEPTION(FailedCreateXioContext);
MAKE_EXCEPTION(FailedRegisterEventHandler);
MAKE_EXCEPTION(FailedCreateXioMempool);

class NetworkXioServer {
public:
  NetworkXioServer(const std::string &transport, 
      const std::string &ipaddr,
      int port,
      int32_t startCoreForIO,
      int32_t numCoresForIO,
      int32_t queueDepthForIO,
      FileTranslatorFunc fileTranslatorFunc, 
      bool newInstance,
      size_t snd_rcv_queue_depth = 256);

  ~NetworkXioServer();

  NetworkXioServer(const NetworkXioServer &) = delete;

  NetworkXioServer &operator=(const NetworkXioServer &) = delete;

  int on_request(xio_session *session, xio_msg *req, int last_in_rxq,
                 void *cb_user_context);

  int on_session_event(xio_session *session,
                       xio_session_event_data *event_data,
                       void* cb_user_context);

  int on_new_session(xio_session *session, xio_new_session_req *req);

  int on_msg_send_complete(xio_session *session, xio_msg *msg,
                           void *cb_user_context);

  int on_msg_error(xio_session *session, xio_status error,
                   xio_msg_direction direction, xio_msg *msg);

  int assign_data_in_buf(xio_msg *msg);

  void run(std::promise<void> &promise);

  void shutdown();

  void send_reply(NetworkXioRequest *req);

  void evfd_stop_loop(int fd, int events, void *data);

  static void destroy_ctx_shutdown(xio_context *ctx);

private:
  //    DECLARE_LOGGER("NetworkXioServer");
  //

  std::string transport_;
  std::string ipaddr_;
  int port_{-1};

  // one for each portal
  // not using smart ptrs because it will interfere with accelio shutdown logic
  // TODO : free this memory 
  std::vector<PortalThreadData*> ptVec_;

  int32_t startCoreForIO_{0};
  int32_t numCoresForIO_{0};
  int32_t queueDepthForIO_{0};

  FileTranslatorFunc fileTranslatorFunc_{nullptr};

  bool newInstance_{false};

public: // TODO
  bool stopping{false};
  bool stopped{false};
private:

  friend class NetworkXioIOHandler; // access serviceHandle_
  friend class PortalThreadData; // access mpool

  std::mutex mutex_;
  std::condition_variable cv_;

  EventFD evfd;

  int queue_depth{0};

  std::shared_ptr<xio_context> ctx;
  std::shared_ptr<xio_server> server;
  std::shared_ptr<xio_mempool> xio_mpool;

  int create_session_connection(xio_session *session,
                                xio_session_event_data *event_data,
                                PortalThreadData* cd);

  void destroy_session_connection(
    xio_session *session ATTRIBUTE_UNUSED, 
    xio_session_event_data *evdata,
    PortalThreadData* cd);

  void deallocate_request(NetworkXioRequest *req);

};
}
} // namespace
