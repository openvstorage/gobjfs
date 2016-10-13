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

class NetworkXioMsg {
public:
  explicit NetworkXioMsg(NetworkXioMsgOpcode opcode = NetworkXioMsgOpcode::Noop)
      : opcode_(opcode) {}

public:
  NetworkXioMsgOpcode opcode_;

  // sent from client to server
  std::vector<std::string> filenameVec_;
  std::vector<size_t> sizeVec_;
  std::vector<uint64_t> offsetVec_;

  // sent from server to client
  std::vector<ssize_t> retvalVec_;
  std::vector<int> errvalVec_;
  
  // sent from client to server and reflected back
  // this points to xio_msg_s allocated on client
  std::vector<uintptr_t> opaqueVec_;

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

    opaqueVec_.clear();

    filenameVec_.clear();
    sizeVec_.clear();
    offsetVec_.clear();

    retvalVec_.clear();
    errvalVec_.clear();
  }

public:
  MSGPACK_DEFINE(opcode_, 
      filenameVec_, sizeVec_, offsetVec_, 
      retvalVec_, errvalVec_, 
      opaqueVec_);
};
}
}

// compilation errors ensue if you put this macro inside namespace
MSGPACK_ADD_ENUM(gobjfs::xio::NetworkXioMsgOpcode);
