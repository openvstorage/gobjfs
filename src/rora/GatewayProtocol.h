#pragma once

#include <msgpack.hpp>
#include <string>
#include <sstream>
#include <boost/interprocess/managed_shared_memory.hpp>

#include <util/lang_utils.h>

namespace bip = boost::interprocess;

namespace gobjfs {
namespace rora {

enum Opcode {
  INVALID = 0,
  OPEN,
  READ,
  CLOSE
};

struct GatewayMsg {

  Opcode opcode_{Opcode::INVALID};
  int edgePid_{-1};

  std::string filename_;
  size_t size_{0};
  off_t offset_{0};

  // handle is a difference_type
  bip::managed_shared_memory::handle_t buf_;

  ssize_t retval_{-1};
  int errval_{-1};

  public:

  explicit GatewayMsg() {}

  GOBJFS_DISALLOW_COPY(GatewayMsg);
  GOBJFS_DISALLOW_MOVE(GatewayMsg);

  const std::string pack() {
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
      filename_,
      size_,
      offset_,
      buf_,
      retval_,
      errval_);
  
};

}
}

MSGPACK_ADD_ENUM(gobjfs::rora::Opcode);


