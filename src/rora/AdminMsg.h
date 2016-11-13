#pragma once

#include <msgpack.hpp>
#include <string>
#include <sstream>

#include <util/lang_utils.h>

namespace gobjfs {
namespace rora {

#define REQ_START 1000
#define RESP_START (REQ_START + 1000)

enum AdminOpcode {
  INVALID = 0,

  ADD_EDGE_REQ = REQ_START,
  DROP_EDGE_REQ,
  ADD_ASD_REQ,
  DROP_ASD_REQ,

  ADD_EDGE_RESP = RESP_START,
  DROP_EDGE_RESP,
  ADD_ASD_RESP,
  DROP_ASD_RESP,
};

struct AdminMsg {

  // MaxMsgSize is used during message queue creation
  // this param must be large enough to fit msgpack
  // of filename and other items in this structure
  static const size_t MaxMsgSize;

  AdminOpcode opcode_{AdminOpcode::INVALID};

  // set when adding or dropping edge
  int edgePid_{-1};
  // how much outstanding IO is this edge expected to send
  size_t maxOutstanding_{1};

  // set when adding or dropping ASD
  std::string transport_;
  std::string ipAddress_;
  int port_;

  // set in response
  int retval_{-1};

  public:

  const std::string pack() const {
    std::stringstream sbuf;
    msgpack::pack(sbuf, *this);
    return sbuf.str();
  }

  void unpack(const char* sbuf, const size_t size) {
    msgpack::unpacked msg;
    msgpack::unpack(&msg, sbuf, size);
    msgpack::object obj = msg.get();
    obj.convert(this);
  }

  public:

  MSGPACK_DEFINE(opcode_,
      edgePid_,
      transport_,
      ipAddress_,
      port_,
      retval_);
};

AdminMsg createAddEdgeRequest(size_t maxOutstanding);
AdminMsg createDropEdgeRequest();

AdminMsg createAddASDRequest(const std::string& transport,
    const std::string& ipAddress,
    int port);
AdminMsg createDropASDRequest(const std::string& transport,
    const std::string& ipAddress,
    int port);

}
}

MSGPACK_ADD_ENUM(gobjfs::rora::AdminOpcode);


