#include "GatewayProtocol.h"
#include <rora/EdgeQueue.h>

namespace gobjfs {
namespace rora {

const size_t GatewayMsg::MaxMsgSize = 1024;
 
GatewayMsg::~GatewayMsg() {
  // check segment was deallocated in case of READ
  //assert(rawbuf_ == nullptr);
  //assert(buf_ == 0);
}

GatewayMsg createOpenRequest() {
  GatewayMsg gmsg;
  gmsg.opcode_ = Opcode::OPEN;
  gmsg.edgePid_ = getpid();
  return gmsg;
}

GatewayMsg createReadRequest(
    EdgeQueue* edgeQueue,
    uint32_t fileNumber,
    const std::string& filename, 
    off_t offset, 
    size_t size) {

  GatewayMsg gmsg;
  gmsg.opcode_ = Opcode::READ;
  gmsg.edgePid_ = getpid();
  gmsg.fileNumber_ = fileNumber;
  gmsg.filename_ = filename;
  gmsg.offset_ = offset;
  gmsg.size_ = size;

  gmsg.rawbuf_ = edgeQueue->alloc(size);
  gmsg.buf_ = edgeQueue->segment_->get_handle_from_address(gmsg.rawbuf_);
  return gmsg;
}

GatewayMsg createCloseRequest() {
  GatewayMsg gmsg;
  gmsg.opcode_ = Opcode::CLOSE;
  gmsg.edgePid_ = getpid();
  return gmsg;
}

}
}
