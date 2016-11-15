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

GatewayMsg createReadResponse(EdgeQueue* edgePtr,
  gobjfs::xio::giocb* iocb,
  int retval) {
  GatewayMsg respMsg;
  edgePtr->GatewayMsg_from_giocb(respMsg, *iocb, retval);
  return respMsg;
}

GatewayMsg createInvalidResponse(int pid, int retval) {
  GatewayMsg adminResp;
  adminResp.opcode_ = Opcode::BAD_OPCODE_RESP;
  return adminResp;
}

GatewayMsg createAddEdgeRequest(size_t maxOutstanding) {
  GatewayMsg adminMsg;
  adminMsg.opcode_ = Opcode::ADD_EDGE_REQ;
  adminMsg.edgePid_ = getpid();
  adminMsg.maxOutstanding_ = maxOutstanding;
  return adminMsg;
}

GatewayMsg createAddEdgeResponse(int pid, int retval) {
  GatewayMsg adminResp;
  adminResp.opcode_ = Opcode::ADD_EDGE_RESP;
  return adminResp;
}

GatewayMsg createDropEdgeRequest() {
  GatewayMsg adminMsg;
  adminMsg.opcode_ = Opcode::DROP_EDGE_REQ;
  adminMsg.edgePid_ = getpid();
  return adminMsg;
}

GatewayMsg createDropEdgeResponse(int pid, int retval) {
  GatewayMsg adminResp;
  adminResp.opcode_ = Opcode::DROP_EDGE_RESP;
  return adminResp;
}

GatewayMsg createAddASDRequest(const std::string& transport,
    const std::string& ipAddress,
    int port) {
  GatewayMsg adminMsg;
  adminMsg.opcode_ = Opcode::ADD_ASD_REQ;
  adminMsg.transport_ = transport;
  adminMsg.ipAddress_ = ipAddress;
  adminMsg.port_ = port;
  adminMsg.edgePid_ = getpid();
  return adminMsg;
}

GatewayMsg createAddASDResponse(int retval) {
  GatewayMsg adminResp;
  adminResp.opcode_ = Opcode::ADD_ASD_RESP;
  return adminResp;
}

GatewayMsg createDropASDRequest(const std::string& transport,
    const std::string& ipAddress,
    int port) {
  GatewayMsg adminMsg;
  adminMsg.opcode_ = Opcode::DROP_ASD_REQ;
  adminMsg.transport_ = transport;
  adminMsg.ipAddress_ = ipAddress;
  adminMsg.port_ = port;
  adminMsg.edgePid_ = getpid();
  return adminMsg;
}

GatewayMsg createDropASDResponse(int retval) {
  GatewayMsg adminResp;
  adminResp.opcode_ = Opcode::DROP_ASD_RESP;
  return adminResp;
}

}
}
