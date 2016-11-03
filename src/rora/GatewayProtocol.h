#pragma once

#include <msgpack.hpp>
#include <string>
#include <sstream>
#include <boost/interprocess/managed_shared_memory.hpp>

#include <util/lang_utils.h>

namespace bip = boost::interprocess;

namespace gobjfs {
namespace rora {

#define REQ_START 1000
#define RESP_START (REQ_START + 1000)

enum Opcode {
  INVALID = 0,

  ADD_EDGE_REQ = REQ_START,
  READ_REQ,
  DROP_EDGE_REQ,

  ADD_EDGE_RESP = RESP_START,
  READ_RESP,
  DROP_EDGE_RESP,
};

class EdgeQueue;

struct GatewayMsg {

  // MaxMsgSize is used during message queue creation
  // this param must be large enough to fit msgpack
  // of filename and other items in this structure
  static const size_t MaxMsgSize;

  Opcode opcode_{Opcode::INVALID};
  int edgePid_{-1};

  // added file number for mem check during benchmark
  // this field doesnt have any inherent functional purpose
  int32_t fileNumber_{-1};

  size_t numElems_{0};

  // file, size, offset for the read request
  std::vector<std::string> filenameVec_;
  std::vector<size_t> sizeVec_;
  std::vector<off_t> offsetVec_;

  // handle is a difference_type (i.e offset within shmem segment)
  std::vector<bip::managed_shared_memory::handle_t> bufVec_;

  // rawbuf deliberately not included in msgpack
  std::vector<void*> rawbufVec_;

  // if read succeeded, this is size of block read 
  // if read failed, it is errno
  std::vector<ssize_t> retvalVec_;

  public:

  explicit GatewayMsg(const size_t numRequests = 0);

  ~GatewayMsg();

  size_t numElems() const { return numElems_; }

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
      fileNumber_,
      numElems_,
      filenameVec_,
      sizeVec_,
      offsetVec_,
      bufVec_,
      retvalVec_);
  
};

GatewayMsg createOpenRequest();

GatewayMsg createReadRequest(
    EdgeQueue* edgeQueue,
    uint32_t fileNumber,
    const std::string& filename, 
    off_t offset, 
    size_t size);

GatewayMsg createReadRequest(
    EdgeQueue* edgeQueue,
    uint32_t fileNumber,
    const std::vector<std::string> &filenameVec, 
    std::vector<off_t> &offsetVec, 
    std::vector<size_t> &sizeVec);

GatewayMsg createCloseRequest();

}
}

MSGPACK_ADD_ENUM(gobjfs::rora::Opcode);


