#include "ASDQueue.h"
#include <rora/GatewayProtocol.h>
#include <string>
#include <gobjfs_log.h>
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

  isCreator_ = true;

  queueName_ = getASDQueueName(uri);

  // always remove previous ASD queue
  remove(uri);

  try {
    mq_ = gobjfs::make_unique<bip::message_queue>(bip::create_only, 
      queueName_.c_str(),
      maxQueueLen,
      maxMsgSize);

    LOG(INFO) << "created asd queue=" << queueName_ 
      << ",maxQueueLen=" << maxQueueLen
      << ",maxMsgSize=" << maxMsgSize_;
  
  } catch (const std::exception& e) {

    mq_.reset();

    throw std::runtime_error(
        "failed to create edgequeue for uri=" + uri);
  }
}


/**
 * this is an open
 */
ASDQueue::ASDQueue(const std::string &uri) {

  isCreator_ =  false;

  queueName_ = getASDQueueName(uri);

  try {
    mq_ = gobjfs::make_unique<bip::message_queue>(bip::open_only, queueName_.c_str());
    maxMsgSize_ = getMaxMsgSize();
    LOG(INFO) << "opened asd queue=" << queueName_ 
      << " with maxMsgSize=" << maxMsgSize_;
  } catch (const std::exception& e) {
    mq_.reset();
    throw std::runtime_error(
        "failed to open edgequeue for uri=" + uri);
  }
}

int ASDQueue::remove(const std::string& uri) {

  auto queueName_ = getASDQueueName(uri);
  auto ret = bip::message_queue::remove(queueName_.c_str());
  if (ret == false) {
    LOG(ERROR) << "Failed to remove message queue=" << queueName_;
  }
  return ret;
}

ASDQueue::~ASDQueue() {

  if (isCreator_) {
    if (mq_) {
      auto ret = bip::message_queue::remove(queueName_.c_str());
      if (ret == false) {
        LOG(ERROR) << "Failed to remove message queue=" << queueName_;
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
    mq_->send(sendStr.c_str(), sendStr.size(), 0);
    updateStats(writeStats_, sendStr.size());
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
    mq_->receive(buf, maxMsgSize_, recvdSize, priority);
    gmsg.unpack(buf, recvdSize);
    updateStats(readStats_, recvdSize);
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
    bool ret = mq_->try_receive(buf, maxMsgSize_, recvdSize, priority);
    if (ret == true) {
      gmsg.unpack(buf, recvdSize);
      updateStats(readStats_, recvdSize);
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
    bool ret = mq_->timed_receive(buf, maxMsgSize_, recvdSize, priority,
        now + boost::posix_time::milliseconds(millisec));
    if (ret == true) {
      gmsg.unpack(buf, recvdSize);
      updateStats(readStats_, recvdSize);
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
  s 
    << "read={count=" << readStats_.count_ << ",msg_size=" << readStats_.msgSize_ << "}"
    << ",write={count=" << writeStats_.count_ << ",msg_size=" << writeStats_.msgSize_ << "}"
    ;
  return s.str();
}

void ASDQueue::clearStats() {
  readStats_.count_ = 0;
  readStats_.msgSize_.reset();
  writeStats_.count_ = 0;
  writeStats_.msgSize_.reset();
}

const std::string& ASDQueue::getName() const {
  return queueName_;
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
