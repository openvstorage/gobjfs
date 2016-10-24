#include "GatewayProtocol.h"
#include <rora/EdgeQueue.h>

namespace gobjfs {
namespace rora {

int createOpenRequest(GatewayMsg& gmsg) {
  gmsg.opcode_ = Opcode::OPEN;
  gmsg.edgePid_ = getpid();
  return 0;
}

int createReadRequest(GatewayMsg& gmsg, 
    EdgeQueue* edgeQueue,
    const std::string& filename, 
    off_t offset, 
    size_t size) {

  gmsg.opcode_ = Opcode::READ;
  gmsg.edgePid_ = getpid();
  gmsg.filename_ = filename;
  gmsg.offset_ = offset;
  gmsg.size_ = size;

  gmsg.rawbuf_ = edgeQueue->alloc(size);
  gmsg.buf_ = edgeQueue->segment_->get_handle_from_address(gmsg.rawbuf_);
}

int createCloseRequest(GatewayMsg& gmsg) {
  gmsg.opcode_ = Opcode::CLOSE;
  gmsg.edgePid_ = getpid();
  return 0;
}

}
}
