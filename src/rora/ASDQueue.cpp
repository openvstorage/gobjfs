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

  created_ = true;

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

  created_ =  false;

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
  bip::message_queue::remove(queueName_.c_str());

  return 0;
}

ASDQueue::~ASDQueue() {

  if (created_) {
    if (mq_) {
      auto ret = bip::message_queue::remove(queueName_.c_str());
      if (ret == false) {
        LOG(ERROR) << "Failed to remove message queue=" << queueName_;
      }
    }
  }
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

int ASDQueue::try_read(GatewayMsg& gmsg) {
  uint32_t priority;
  size_t recvdSize;
  try {
    char buf[maxMsgSize_];
    bool ret = mq_->try_receive(buf, maxMsgSize_, recvdSize, priority);
    if (ret == true) {
      gmsg.unpack(buf, recvdSize);
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
      return 0;
    } else {
      return -EAGAIN;
    }
  } catch (const std::exception& e) {
    return -1;
  }
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
