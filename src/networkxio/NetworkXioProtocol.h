// Copyright (C) 2016 iNuron NV
//
// This file is part of Open vStorage Open Source Edition (OSE),
// as available from
//
//      http://www.openvstorage.org and
//      http://www.openvstorage.com.
//
// This file is free software; you can redistribute it and/or modify it
// under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
// as published by the Free Software Foundation, in version 3 as it comes in
// the LICENSE.txt file of the Open vStorage OSE distribution.
// Open vStorage is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY of any kind.

#pragma once

#include <msgpack.hpp>
#include <sstream>
#include <vector>

#include <networkxio/NetworkXioCommon.h>

namespace gobjfs {
namespace xio {

// assume each request requires 256 byte header (i.e. length of filename)
static constexpr size_t MAX_INLINE_HEADER_OR_DATA = 256;

/**
 *  DATA MODEL
 *
 *  N giocb in user thread
 *  map to
 *    |
 *    |
 *  N aio_request on NetworkXioClient
 *  map to
 *    |
 *    |
 *  1 ClientMsg on NetworkXioClient
 *  map to
 *    |
 *    | 
 *  1 NetworkXioMsg on accelio transport
 *  map to
 *    |
 *    | 
 *  1 NetworkXioRequest on NetworkXioServer
 *  map to
 *    |
 *    | 
 *  N gIOBatch on IOExecFile
 *  map to
 *    |
 *    | 
 *  N FilerJob on IOExecutor
 */

/**
 * The header of the xio_msg is packed using msgpack
 *   i.e. xio_msg.in.header.iov_base = (msgpack buffer)
 * When the receiver unpack this header from an xio_msg, it does not know the opcode
 * so it cannot tell apriori which structure is being unpacked.
 *
 * Therefore, all msgpack messages are unpacked into fields of one NetworkXioMsg.
 * This is a single structure which is union of fields for all possible
 * messages in the network protocol.
 * While sending from client to server {filename, offset, size} are set
 * When sending from server to client {errval, retval} are set
 * This waste some spaces but not too much since the std::vectors are dynamically sized
 */

class NetworkXioMsg {
public:
  explicit NetworkXioMsg(NetworkXioMsgOpcode opcode = NetworkXioMsgOpcode::Noop)
      : opcode_(opcode) {}

public:
  NetworkXioMsgOpcode opcode_;

  size_t numElems_{0};

  // sent from client to server
  std::vector<std::string> filenameVec_;
  std::vector<size_t> sizeVec_;
  std::vector<uint64_t> offsetVec_;

  // sent from server to client
  std::vector<ssize_t> retvalVec_;
  std::vector<int> errvalVec_;
  
  // ptr to ClientMsg allocated on client-side
  // sent from client to server and reflected back
  uintptr_t clientMsgPtr_{0};

public:
  NetworkXioMsg(const NetworkXioMsg& other) = delete;
  NetworkXioMsg(NetworkXioMsg&& other) = delete;
  void operator = (const NetworkXioMsg& other) = delete;
  void operator = (NetworkXioMsg&& other) = delete;

  const NetworkXioMsgOpcode &opcode() const { return opcode_; }

  void opcode(const NetworkXioMsgOpcode &op) { opcode_ = op; }

  const std::string pack_msg() const {
    std::stringstream sbuf;
    msgpack::pack(sbuf, *this);
    return sbuf.str();
  }

  void unpack_msg(const char *sbuf, const size_t size) {
    msgpack::unpacked msg;
    msgpack::unpack(&msg, sbuf, size);
    msgpack::object obj = msg.get();
    obj.convert(this);
  }

  void unpack_msg(const std::string &sbuf) {
    msgpack::unpacked msg;
    msgpack::unpack(&msg, sbuf.data(), sbuf.size());
    msgpack::object obj = msg.get();
    obj.convert(this);
  }

  void clear() {
    opcode_ = NetworkXioMsgOpcode::Noop;

    numElems_ = 0;
    clientMsgPtr_ = 0;

    filenameVec_.clear();
    sizeVec_.clear();
    offsetVec_.clear();

    retvalVec_.clear();
    errvalVec_.clear();
  }

public:
  MSGPACK_DEFINE(opcode_, 
      numElems_, 
      filenameVec_, sizeVec_, offsetVec_, 
      retvalVec_, errvalVec_, 
      clientMsgPtr_);
};
}
}

// compilation errors ensue if you put this macro inside namespace
MSGPACK_ADD_ENUM(gobjfs::xio::NetworkXioMsgOpcode);
