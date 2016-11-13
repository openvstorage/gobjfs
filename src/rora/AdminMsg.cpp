#include "AdminMsg.h"
#include <unistd.h>

namespace gobjfs {
namespace rora {

const size_t AdminMsg::MaxMsgSize = 1024;

AdminMsg createAddEdgeRequest(size_t maxOutstanding) {
  AdminMsg adminMsg;
  adminMsg.opcode_ = AdminOpcode::ADD_EDGE_REQ;
  adminMsg.edgePid_ = getpid();
  adminMsg.maxOutstanding_ = maxOutstanding;
  return adminMsg;
}

AdminMsg createDropEdgeRequest() {
  AdminMsg adminMsg;
  adminMsg.opcode_ = AdminOpcode::DROP_EDGE_REQ;
  adminMsg.edgePid_ = getpid();
  return adminMsg;
}

AdminMsg createAddASDRequest(const std::string& transport,
    const std::string& ipAddress,
    int port) {
  AdminMsg adminMsg;
  adminMsg.opcode_ = AdminOpcode::ADD_ASD_REQ;
  adminMsg.transport_ = transport;
  adminMsg.ipAddress_ = ipAddress;
  adminMsg.port_ = port;
  return adminMsg;
}

AdminMsg createDropASDRequest(const std::string& transport,
    const std::string& ipAddress,
    int port) {
  AdminMsg adminMsg;
  adminMsg.opcode_ = AdminOpcode::DROP_ASD_REQ;
  adminMsg.transport_ = transport;
  adminMsg.ipAddress_ = ipAddress;
  adminMsg.port_ = port;
  return adminMsg;
}

}
}
