#include <rora/GatewayClient.h>

#include <unistd.h> // getpid
#include <sstream>  // ostringstream
#include <gobjfs_log.h> // logger
#include <rora/GatewayProtocol.h>

namespace gobjfs {
namespace rora {

void EdgeIORequest::append(EdgeIORequest& other) {

    std::move(std::begin(other.eiocbVec_),
         std::end(other.eiocbVec_),
         std::back_inserter(eiocbVec_));
    std::move(std::begin(other.retvalVec_),
         std::end(other.retvalVec_),
         std::back_inserter(retvalVec_));
    other.eiocbVec_.clear();
    other.retvalVec_.clear();
    asdIdx_ = other.asdIdx_;
}

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

std::string GatewayClient::getStats() {
    std::ostringstream os;
    for (auto& entry : stats_) {
        os << "{asdid=" << entry.first 
            << ",submitted=" << entry.second.numSubmitted_
            << ",completed=" << entry.second.numCompleted_
            << "},";
    }
    return os.str();
}

GatewayClient::ASDId 
GatewayClient::addASD(std::string transport, std::string ipAddress, int port) {

    for (auto& entry : asdQueueVec_) {
        ASDQueue* asdPtr = entry.get();
        if ((asdPtr->transport_ == transport) && 
            (asdPtr->ipAddress_ == ipAddress) && 
            (asdPtr->port_ == port)) {
            LOG(ERROR) << "asd exists for destination=" << transport << "://" << ipAddress << ":" << port;
            return -EEXIST;
        }
    }

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

int GatewayClient::read(EdgeIORequest& req) {
    const auto numSubmitted = req.size();
    auto ret = asyncRead(req);
    if (ret == 0) {
        EdgeIORequest completedReq;
        // keep calling waitForResponse as long as there
        // are outstanding requests
        while (req.size() != completedReq.size()) {
            EdgeIORequest oneReq;
            ret = waitForResponse(oneReq);
            if (ret != 0) {
                LOG(ERROR) << "failed to get responses for all requests";
                break;
            }
            completedReq.append(oneReq);
        }
    }
    return ret;
}

int GatewayClient::asyncRead(EdgeIORequest& req) {
    auto asdPtr = asdQueueVec_.at(req.asdIdx_).get();
    auto ret = asdPtr->write(createReadRequest(edgeQueue_.get(), req));
    if (ret == 0) {
        stats_[req.asdIdx_].numSubmitted_ += req.size();
    }
    return ret;
}

int GatewayClient::release(EdgeIORequest& completedReq) {
    for (auto& eiocb : completedReq.eiocbVec_) {
        edgeQueue_->free(eiocb->buffer_);
    }
    return 0;
}

int GatewayClient::waitForResponse(EdgeIORequest& completedReq) {

    GatewayMsg responseMsg(1);
    const auto ret = edgeQueue_->readResponse(responseMsg);
    assert(ret == 0);

    for (size_t idx = 0; idx < responseMsg.numElems(); idx ++) {
        EdgeIOCB* iocb = new EdgeIOCB;;
        completedReq.retvalVec_.push_back(responseMsg.retvalVec_[idx]);
        iocb->filename_ = responseMsg.filenameVec_[idx];
        iocb->size_ = responseMsg.sizeVec_[idx];
        iocb->offset_ = responseMsg.offsetVec_[idx];
        iocb->buffer_ = responseMsg.rawbufVec_[idx];
        completedReq.eiocbVec_.push_back(std::unique_ptr<EdgeIOCB>(iocb));
    }
    stats_[completedReq.asdIdx_].numCompleted_ += completedReq.size();
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
