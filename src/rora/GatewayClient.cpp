#include <rora/GatewayClient.h>

#include <unistd.h> // getpid
#include <rora/GatewayProtocol.h>

namespace gobjfs {
namespace rora {

GatewayClient::GatewayClient(int32_t roraVersion,
  size_t maxOutstandingIO,
  size_t blockSize) {

    roraVersion_ = roraVersion;
    assert(roraVersion_ != -1);

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

int GatewayClient::addASD(std::string transport, std::string ipAddress, int port) {

    assert(roraVersion_ != -1);

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
    return asdQueueVec_.size() - 1;
}

int GatewayClient::read(eioRequest& req) {
    return 0;
}

int GatewayClient::asyncRead(eioRequest& req) {
    auto asdPtr = asdQueueVec_.at(req.asdIdx_).get();
    return asdPtr->write(createReadRequest(edgeQueue_.get(), req));
    return 0;
}

int GatewayClient::release(eioRequest& completedReq) {
    for (auto& eiocb : completedReq.eiocbVec_) {
        edgeQueue_->free(eiocb->buffer_);
    }
    return 0;
}

int GatewayClient::waitForResponse(eioRequest& completedReq) {

    GatewayMsg responseMsg(1);
    const auto ret = edgeQueue_->readResponse(responseMsg);
    assert(ret == 0);

    for (size_t idx = 0; idx < responseMsg.numElems(); idx ++) {
        eiocb* iocb = new eiocb;;
        completedReq.retvalVec_.push_back(responseMsg.retvalVec_[idx]);
        iocb->filename_ = responseMsg.filenameVec_[idx];
        iocb->size_ = responseMsg.sizeVec_[idx];
        iocb->offset_ = responseMsg.offsetVec_[idx];
        iocb->buffer_ = responseMsg.rawbufVec_[idx];
        completedReq.eiocbVec_.push_back(iocb);
    }
    return 0;
}

int GatewayClient::shutdown() {

    int ret = 0;
    assert(roraVersion_ != -1);

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

GatewayClient::~GatewayClient() {
    if (not asdQueueVec_.empty() && adminQueue_ && edgeQueue_) {
        shutdown();
    }
}

}}
