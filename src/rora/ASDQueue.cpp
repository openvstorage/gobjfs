#include "ASDQueue.h"
#include <rora/GatewayProtocol.h>
#include <string>
#include <glog/logging.h>
#include <type_traits>

namespace bip = boost::interprocess;

namespace gobjfs {
namespace rora {

static std::string getASDQueueName(const std::string& uri) {
  std::string str = "to_asd_" + uri;
  return str;
}

/**
 * this is a create
 */
ASDQueue::ASDQueue(const std::string& uri,
    size_t maxQueueLen, 
    size_t maxMsgSize) :
  maxMsgSize_(maxMsgSize) {

  created_ = true;

  queueName_ = getASDQueueName(uri);

  // always remove previous ASD queue
  remove(uri);

  try {
    mq_ = new bip::message_queue(bip::create_only, 
      queueName_.c_str(),
      maxQueueLen,
      maxMsgSize);
  
  } catch (const std::exception& e) {

    delete mq_;
    mq_ = nullptr;

    throw std::runtime_error(
        "failed to create edgequeue for uri=" + uri);
  }
}


/**
 * this is an open
 */
ASDQueue::ASDQueue(const std::string &uri) {

  created_ =  false;

  queueName_ = getASDQueueName(uri);

  try {
    mq_ = new bip::message_queue(bip::open_only, queueName_.c_str());
    maxMsgSize_ = getMaxMsgSize();
  } catch (const std::exception& e) {
    delete mq_;
    mq_ = nullptr;
    throw std::runtime_error(
        "failed to open edgequeue for uri=" + uri);
  }
}

int ASDQueue::remove(const std::string& uri) {

  auto queueName_ = getASDQueueName(uri);
  bip::message_queue::remove(queueName_.c_str());

  return 0;
}

ASDQueue::~ASDQueue() {

  delete mq_;

  if (created_) {
    if (mq_) {
      auto ret = bip::message_queue::remove(queueName_.c_str());
      if (ret == false) {
        LOG(ERROR) << "Failed to remove message queue=" << queueName_;
      }
    }
  }

  mq_ = nullptr;
}

/**
 * TODO : can throw
 */
int ASDQueue::write(const GatewayMsg& gmsg) {
  try {
    auto sendStr = gmsg.pack();
    assert(sendStr.size() < maxMsgSize_);
    mq_->send(sendStr.c_str(), sendStr.size(), 0);
    return 0;
  } catch (const std::exception& e) {
    return -1;
  }
}

/**
 * TODO : can throw
 */
int ASDQueue::read(GatewayMsg& gmsg) {
  uint32_t priority;
  size_t recvdSize;
  try {
    char buf[maxMsgSize_];
    mq_->receive(buf, maxMsgSize_, recvdSize, priority);
    gmsg.unpack(buf, recvdSize);
    return 0;
  } catch (const std::exception& e) {
    return -1;
  }
}
 
size_t ASDQueue::getCurrentQueueLen() const {
  if (!mq_) {
    throw std::runtime_error("invalid edgequeue");
  }
  return mq_->get_num_msg();
}

size_t ASDQueue::getMaxQueueLen() const {
  if (!mq_) {
    throw std::runtime_error("invalid edgequeue");
  }
  return mq_->get_max_msg();
}

size_t ASDQueue::getMaxMsgSize() const {
  if (!mq_) {
    throw std::runtime_error("invalid edgequeue");
  }
  return mq_->get_max_msg_size();
}

}
}
