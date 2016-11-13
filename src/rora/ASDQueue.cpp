#include "ASDQueue.h"
#include <rora/GatewayProtocol.h>
#include <string>
#include <gobjfs_log.h>
#include <type_traits>

namespace bip = boost::interprocess;

namespace gobjfs {
namespace rora {

/**
 * ASDQueue contains rora gateway version
 * this allows running a gateway with new version while old version is active
 */
static std::string getASDQueueName(std::string versionString,
    std::string ipAddress, 
    int port) {

  std::string str = "rora" + versionString 
    + "_to_asd_" + ipAddress + ":" + std::to_string(port);
  return str;
}

/**
 * this is a create
 */
ASDQueue::ASDQueue(const std::string& versionString,
    std::string transport,
    std::string ipAddress,
    int port,
    size_t maxQueueLen, 
    size_t maxMsgSize) :
  versionString_(versionString),
  transport_(transport),
  ipAddress_(ipAddress),
  port_(port) {

  isCreator_ = true;

  queueName_ = getASDQueueName(versionString, ipAddress, port);

  // always remove previous ASD queue
  remove(versionString_, ipAddress, port);

  try {
    mq_ = gobjfs::make_unique<bip::message_queue>(bip::create_only, 
      queueName_.c_str(),
      maxQueueLen,
      maxMsgSize);

    maxMsgSize_ = getMaxMsgSize();
    LOG(INFO) << "created asd queue=" << queueName_ 
      << ",maxQueueLen=" << maxQueueLen
      << ",maxMsgSize=" << maxMsgSize_;
  
  } catch (const std::exception& e) {

    mq_.reset();

    throw std::runtime_error(
        "failed to create edgequeue for uri=" + ipAddress + ":" + std::to_string(port));
  }
}


/**
 * this is an open
 */
ASDQueue::ASDQueue(const std::string& versionString,
    std::string transport,
    std::string ipAddress,
    int port) :
  versionString_(versionString),
  transport_(transport),
  ipAddress_(ipAddress),
  port_(port) {

  isCreator_ =  false;

  queueName_ = getASDQueueName(versionString, ipAddress, port);

  try {
    mq_ = gobjfs::make_unique<bip::message_queue>(bip::open_only, queueName_.c_str());
    maxMsgSize_ = getMaxMsgSize();
    LOG(INFO) << "opened asd queue=" << queueName_ 
      << " with maxMsgSize=" << maxMsgSize_;
  } catch (const std::exception& e) {
    mq_.reset();
    throw std::runtime_error(
        "failed to open edgequeue for uri=" + ipAddress + ":" + std::to_string(port));
  }
}

int ASDQueue::remove(const std::string& versionString, std::string ipAddress, int port) {

  auto queueName_ = getASDQueueName(versionString, ipAddress, port);
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
  assert(maxMsgSize_ > 0);
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
  assert(maxMsgSize_ > 0);
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
  assert(maxMsgSize_ > 0);
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
  assert(maxMsgSize_ > 0);
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
