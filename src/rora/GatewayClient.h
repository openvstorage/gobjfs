#pragma once

#include <unistd.h> // getpid
#include <rora/AdminQueue.h>
#include <rora/EdgeQueue.h>
#include <rora/ASDQueue.h>
#include <rora/GatewayProtocol.h>

namespace gobjfs {
namespace rora {

struct GatewayClient {

  std::string roraVersion_;

  AdminQueueUPtr adminQueue_;

  EdgeQueueUPtr edgeQueue_;

  std::vector<ASDQueueUPtr> asdQueueVec_;

  static GatewayClient* create(std::string roraVersion);

  GatewayClient(std::string roraVersion,
      size_t maxOutstandingIO,
      size_t blockSize) {

    roraVersion_ = roraVersion;

    adminQueue_ = gobjfs::make_unique<AdminQueue>(roraVersion);

    int pid = getpid();
    // create new edgequeue for this process
    edgeQueue_ = gobjfs::make_unique<EdgeQueue>(pid, 2 * maxOutstandingIO, 
        GatewayMsg::MaxMsgSize, blockSize);
  
    // first send ADD_EDGE message to rora gateway 
    int ret = adminQueue_->write(createAddEdgeRequest(maxOutstandingIO));
    assert(ret == 0);
  
    GatewayMsg responseMsg;
    ret = edgeQueue_->readResponse(responseMsg);
    assert(ret == 0);
    assert(responseMsg.opcode_ == Opcode::ADD_EDGE_RESP);
  }

  int addASD(std::string transport, std::string ipAddress, int port) {

    assert(not roraVersion_.empty());

    int ret = adminQueue_->write(createAddASDRequest(transport,
      ipAddress,
      port));
    assert(ret == 0);

    GatewayMsg responseMsg;
    ret = edgeQueue_->readResponse(responseMsg);
    assert(ret == 0);
    assert(responseMsg.opcode_ == Opcode::ADD_ASD_RESP);

    auto asdPtr = gobjfs::make_unique<ASDQueue>(roraVersion_, 
        transport,
        ipAddress,
        port);

    asdQueueVec_.push_back(std::move(asdPtr));

    return 0;
  }

  int read(const char* filename, off_t offset, size_t sz) {

    return 0;
  }

  /**
   * call this in case dtor is inconvenient
   */
  int shutdown() {

    int ret = 0;

    assert(not roraVersion_.empty());
    // drop the asds
    for (auto& asdQueue : asdQueueVec_) {

      ret = adminQueue_->write(createDropASDRequest(asdQueue->transport_,
        asdQueue->ipAddress_,
        asdQueue->port_));
      assert(ret == 0);

      GatewayMsg responseMsg;
      ret = edgeQueue_->readResponse(responseMsg);
      assert(ret == 0);
      assert(responseMsg.opcode_ == Opcode::DROP_ASD_RESP);
    }
    asdQueueVec_.clear();

    // sending close message will cause rora gateway to close
    // the EdgeQueue for sending responses
    ret = adminQueue_->write(createDropEdgeRequest());
    assert(ret == 0);

    GatewayMsg responseMsg;
    ret = edgeQueue_->readResponse(responseMsg);
    assert(ret == 0);
    assert(responseMsg.opcode_ == Opcode::DROP_EDGE_RESP);

    edgeQueue_.reset();
    adminQueue_.reset();
    return 0;
  }

  ~GatewayClient() {
    if (not asdQueueVec_.empty() && adminQueue_ && edgeQueue_) {
      shutdown();
    }
  }

};

}}
