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

#include <iostream>
#include <assert.h>
#include <string>
#include <gobjfs_client.h>
#include <gobjfs_log.h>
#include <util/os_utils.h>
#include <util/Timer.h>

#define ATTRIBUTE_UNUSED __attribute__((unused))

#ifdef FUNC_TRACE
#ifndef XXEnter
#define XXEnter()                                                              \
  std::cout << "Entering function " << __FUNCTION__ << " , " << __FILE__       \
            << " ( " << __LINE__ << " ) " << std::endl;
#endif
#ifndef XXExit
#define XXExit()                                                               \
  std::cout << "Exiting function " << __FUNCTION__ << " , " << __FILE__        \
            << " ( " << __LINE__ << " ) " << std::endl;
#endif

#ifndef XXDone
#define XXDone() goto done;
#endif
#else
#define XXEnter()
#define XXExit()
#define XXDone()
#endif

#define GLOG_ERROR(msg) LOG_ERROR << msg
#define GLOG_FATAL(msg) LOG_FATAL << msg
#define GLOG_INFO(msg) LOG_INFO << msg
#define GLOG_DEBUG(msg) LOG_DEBUG << msg
#define GLOG_TRACE(msg) LOG_TRACE << msg

#define MAKE_EXCEPTION(A)                                                      \
  class A : public std::exception {                                            \
  public:                                                                      \
    A(std::string AA) {}                                                       \
  }

static constexpr uint64_t SEC_TO_NANOSEC = 1000000000;

namespace gobjfs {
namespace xio {

enum class RequestOp {
  Noop,
  Read,
};

enum class TransportType {
  Error,
  SharedMemory, // not supported
  TCP,
  RDMA,
};

struct client_ctx_attr {
  TransportType transport;
  std::string host;
  int port{-1};
};

struct gbuffer {
  void *buf{nullptr};
  size_t size{0};
};

extern void aio_complete_request(void *request, ssize_t retval, int errval);

struct aio_request {
  struct giocb *giocbp{nullptr};
  RequestOp _op{RequestOp::Noop};
  bool _on_suspend{false};
  bool _completed{false};
  bool _failed{false};
  int _errno{0};
  ssize_t _rv{0};

  int64_t _rtt_nanosec{0};
  gobjfs::stats::Timer _timer;
};
}
}
