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

class EdgeQueue;

struct GatewayMsg {

  // MaxMsgSize is used during message queue creation
  // this param must be large enough to fit msgpack
  // of filename and other items in this structure
  static const size_t MaxMsgSize;

  Opcode opcode_{Opcode::INVALID};
  int edgePid_{-1};

  std::string filename_;
  size_t size_{0};
  off_t offset_{0};

  // handle is a difference_type (i.e offset within shmem segment)
  bip::managed_shared_memory::handle_t buf_{0};

  // rawbuf deliberately not included in msgpack
  void* rawbuf_{nullptr}; 

  ssize_t retval_{-1};
  int errval_{-1};

  public:

  explicit GatewayMsg() {}

  ~GatewayMsg();

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
      filename_,
      size_,
      offset_,
      buf_,
      retval_,
      errval_);
  
};

GatewayMsg createOpenRequest();

GatewayMsg createReadRequest(
    EdgeQueue* edgeQueue,
    const std::string& filename, 
    off_t offset, 
    size_t size);

GatewayMsg createCloseRequest();

}
}

MSGPACK_ADD_ENUM(gobjfs::rora::Opcode);


