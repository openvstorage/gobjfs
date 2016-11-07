#include "ASDQueue.h"
#include <rora/GatewayProtocol.h>
#include <string>
#include <gobjfs_log.h>
#include <type_traits>

namespace bip = boost::interprocess;

namespace gobjfs {
namespace rora {

static std::string getASDQueueName(const std::string& uri, size_t idx) {
  std::string str = std::to_string(idx) + "to_asd_" + uri;
  return str;
}

/**
 * this is a create
 */
ASDQueue::ASDQueue(const std::string& uri,
    size_t numQueues,
    size_t maxQueueLen, 
    size_t maxMsgSize) 
  : numQueues_(numQueues)
  , maxMsgSize_(maxMsgSize) {

  isCreator_ = true;

  // always remove previous ASD queue
  remove(uri, numQueues);

  for (size_t idx = 0; idx < numQueues_; idx ++) {

    BaseQueue b;
    b.name_ = getASDQueueName(uri, idx);

    try {
      b.mq_ = gobjfs::make_unique<bip::message_queue>(bip::create_only, 
        b.name_.c_str(),
        maxQueueLen,
        maxMsgSize);

      LOG(INFO) << "created asd queue=" << b.name_ 
        << ",maxQueueLen=" << maxQueueLen
        << ",maxMsgSize=" << maxMsgSize_;
    
      queueVec_.push_back(std::move(b));

    } catch (const std::exception& e) {

      b.mq_.reset();

      throw std::runtime_error(
          "failed to create edgequeue for uri=" + uri);
    }
  }
}


/**
 * this is an open
 */
ASDQueue::ASDQueue(const std::string &uri, size_t numQueues) 
  : numQueues_(numQueues) {

  isCreator_ =  false;

  for (size_t idx = 0; idx < numQueues_; idx ++) {

    BaseQueue b;
    b.name_ = getASDQueueName(uri, idx);

    try {
      b.mq_ = gobjfs::make_unique<bip::message_queue>(bip::open_only, b.name_.c_str());
      maxMsgSize_ = getMaxMsgSize();

      LOG(INFO) << "opened asd queue=" << b.name_ 
        << " with maxMsgSize=" << maxMsgSize_;

      queueVec_.push_back(std::move(b));

    } catch (const std::exception& e) {
      b.mq_.reset();
      throw std::runtime_error(
          "failed to open edgequeue for uri=" + uri);
    }
  }
}

int ASDQueue::remove(const std::string& uri, size_t numQueues) {

  int ret = 0;
  for (size_t idx = 0; idx < numQueues; idx ++) {
    auto queueName = getASDQueueName(uri, idx);
    ret = bip::message_queue::remove(queueName.c_str());
    if (ret == false) {
      LOG(ERROR) << "Failed to remove message queue=" << queueName;
    }
  }
  return ret;
}

ASDQueue::~ASDQueue() {

  if (isCreator_) {
    for (auto& b : queueVec_) {
      if (b.mq_) {
        auto ret = bip::message_queue::remove(b.name_.c_str());
        if (ret == false) {
          LOG(ERROR) << "Failed to remove message queue=" << b.name_;
        }
      }
    }
  }
}

/**
 */
int ASDQueue::write(const GatewayMsg& gmsg) {
  try {
    auto sendStr = gmsg.pack();
    assert(sendStr.size() < maxMsgSize_);
    queueVec_[0].mq_->send(sendStr.c_str(), sendStr.size(), 0);
    updateStats(queueVec_[0].writeStats_, sendStr.size());
    return 0;
  } catch (const std::exception& e) {
    return -1;
  }
}

/**
 */
int ASDQueue::read(GatewayMsg& gmsg) {
  uint32_t priority;
  size_t recvdSize;
  try {
    char buf[maxMsgSize_];
    queueVec_[0].mq_->receive(buf, maxMsgSize_, recvdSize, priority);
    gmsg.unpack(buf, recvdSize);
    updateStats(queueVec_[0].readStats_, recvdSize);
    return 0;
  } catch (const std::exception& e) {
    return -1;
  }
}

int ASDQueue::try_read(GatewayMsg& gmsg) {
  uint32_t priority;
  size_t recvdSize;
  try {
    char buf[maxMsgSize_];
    bool ret = queueVec_[0].mq_->try_receive(buf, maxMsgSize_, recvdSize, priority);
    if (ret == true) {
      gmsg.unpack(buf, recvdSize);
      updateStats(queueVec_[0].readStats_, recvdSize);
      return 0;
    } else {
      return -EAGAIN;
    }
  } catch (const std::exception& e) {
    return -1;
  }
}

int ASDQueue::timed_read(GatewayMsg& gmsg, int millisec) {
  uint32_t priority;
  size_t recvdSize;
  try {
    char buf[maxMsgSize_];
    boost::posix_time::ptime now(boost::posix_time::second_clock::universal_time()); 
    bool ret = queueVec_[0].mq_->timed_receive(buf, maxMsgSize_, recvdSize, priority,
        now + boost::posix_time::milliseconds(millisec));
    if (ret == true) {
      gmsg.unpack(buf, recvdSize);
      updateStats(queueVec_[0].readStats_, recvdSize);
      return 0;
    } else {
      return -EAGAIN;
    }
  } catch (const std::exception& e) {
    return -1;
  }
}

void ASDQueue::updateStats(Statistics& which, size_t msgSize) {
  which.count_ ++;
  which.msgSize_ = msgSize;
}

std::string ASDQueue::getStats() const {
  std::ostringstream s;
  for (auto& b : queueVec_) {
    s 
      << "read={count=" << b.readStats_.count_ << ",msg_size=" << b.readStats_.msgSize_ << "}"
      << ",write={count=" << b.writeStats_.count_ << ",msg_size=" << b.writeStats_.msgSize_ << "}"
    ;
  }
  return s.str();
}

void ASDQueue::clearStats() {
  for (auto& b : queueVec_) {
    b.readStats_.count_ = 0;
    b.readStats_.msgSize_.reset();
    b.writeStats_.count_ = 0;
    b.writeStats_.msgSize_.reset();
  }
}

const std::string& ASDQueue::getName() const {
  return queueVec_[0].name_;
}
 
size_t ASDQueue::getCurrentQueueLen() const {
  if (not queueVec_[0].mq_) {
    throw std::runtime_error("invalid edgequeue");
  }
  return queueVec_[0].mq_->get_num_msg();
}

size_t ASDQueue::getMaxQueueLen() const {
  if (not queueVec_[0].mq_) {
    throw std::runtime_error("invalid edgequeue");
  }
  return queueVec_[0].mq_->get_max_msg();
}

size_t ASDQueue::getMaxMsgSize() const {
  if (not queueVec_[0].mq_) {
    throw std::runtime_error("invalid edgequeue");
  }
  return queueVec_[0].mq_->get_max_msg_size();
}

}
}
