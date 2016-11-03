#include "GatewayProtocol.h"
#include <rora/EdgeQueue.h>

namespace gobjfs {
namespace rora {

const size_t GatewayMsg::MaxMsgSize = 1024;

GatewayMsg::GatewayMsg(size_t numRequests) 
  : numElems_(numRequests) {

  if (not numElems_) {
    return;
  }

  filenameVec_.reserve(numRequests);
  offsetVec_.reserve(numRequests);
  sizeVec_.reserve(numRequests);
  bufVec_.reserve(numRequests);
  rawbufVec_.reserve(numRequests);
  retvalVec_.reserve(numRequests);

}
 
GatewayMsg::~GatewayMsg() {
  // check segment was deallocated in case of READ_REQ
  //assert(rawbuf_ == nullptr);
  //assert(buf_ == 0);
}

GatewayMsg createOpenRequest() {
  GatewayMsg gmsg(0);
  gmsg.opcode_ = Opcode::ADD_EDGE_REQ;
  gmsg.edgePid_ = getpid();
  return gmsg;
}

GatewayMsg createReadRequest(
    EdgeQueue* edgeQueue,
    uint32_t fileNumber,
    const std::string& filename, 
    off_t offset, 
    size_t size) {

  GatewayMsg gmsg(1);
  gmsg.opcode_ = Opcode::READ_REQ;
  gmsg.edgePid_ = getpid();
  gmsg.fileNumber_ = fileNumber;
  gmsg.filenameVec_.push_back(filename);
  gmsg.offsetVec_.push_back(offset);
  gmsg.sizeVec_.push_back(size);

  auto ptr = edgeQueue->alloc(size);
  gmsg.rawbufVec_.push_back(ptr);
  gmsg.bufVec_.push_back(edgeQueue->segment_->get_handle_from_address(ptr));

  gmsg.numElems_ = gmsg.filenameVec_.size();

  return gmsg;
}

GatewayMsg createReadRequest(
    EdgeQueue* edgeQueue,
    uint32_t fileNumber,
    const std::vector<std::string> &filenameVec, 
    std::vector<off_t> &offsetVec, 
    std::vector<size_t> &sizeVec) {

  GatewayMsg gmsg(filenameVec.size());

  gmsg.opcode_ = Opcode::READ_REQ;
  gmsg.edgePid_ = getpid();
  gmsg.fileNumber_ = fileNumber;

  gmsg.filenameVec_ = filenameVec;
  gmsg.offsetVec_ = offsetVec;
  gmsg.sizeVec_ = sizeVec;

  for (auto sz : sizeVec) {
    auto ptr = edgeQueue->alloc(sz);
    gmsg.rawbufVec_.push_back(ptr);
    gmsg.bufVec_.push_back(edgeQueue->segment_->get_handle_from_address(ptr));
  }

  gmsg.numElems_ = gmsg.filenameVec_.size();

  return gmsg;
}

GatewayMsg createCloseRequest() {
  GatewayMsg gmsg(0);
  gmsg.opcode_ = Opcode::DROP_EDGE_REQ;
  gmsg.edgePid_ = getpid();
  return gmsg;
}

}
}
