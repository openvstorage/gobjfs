#include "AdminQueue.h"
#include <rora/GatewayProtocol.h>
#include <string>
#include <gobjfs_log.h>
#include <type_traits>

namespace bip = boost::interprocess;

namespace gobjfs {
namespace rora {

// append version string to queue name so we can allow
// rora gateways of new version to run while old one
// is still not phased out
static std::string getAdminQueueName(const std::string& version) {
  std::string str = "rora" + version + "_adminqueue";
  return str;
}

/**
 * this is a create
 * @param version uniquely identifies the gateway instance
 *        you can run two instances of gateway with different versions
 */
AdminQueue::AdminQueue(const std::string& version, size_t maxQueueLen) {

  isCreator_ = true;

  queueName_ = getAdminQueueName(version);

  // always remove previous Admin queue
  remove(version);

  try {
    mq_ = gobjfs::make_unique<bip::message_queue>(bip::create_only, 
      queueName_.c_str(),
      maxQueueLen,
      GatewayMsg::MaxMsgSize);

    maxMsgSize_ = getMaxMsgSize();

    LOG(INFO) << "created admin queue=" << queueName_ 
      << ",maxQueueLen=" << maxQueueLen
      << ",maxMsgSize=" << maxMsgSize_;
  
  } catch (const std::exception& e) {

    mq_.reset();

    throw std::runtime_error(
        "failed to create edgequeue for version=" + version);
  }
}


/**
 * this is an open
 */
AdminQueue::AdminQueue(std::string version) {

  isCreator_ =  false;

  queueName_ = getAdminQueueName(version);

  try {
    mq_ = gobjfs::make_unique<bip::message_queue>(bip::open_only, queueName_.c_str());
    maxMsgSize_ = getMaxMsgSize();
    LOG(INFO) << "opened admin queue=" << queueName_ 
      << " with maxMsgSize=" << maxMsgSize_;
  } catch (const std::exception& e) {
    mq_.reset();
    throw std::runtime_error(
        "failed to open edgequeue for version=" + version);
  }
}

int AdminQueue::remove(const std::string& version) {

  auto queueName_ = getAdminQueueName(version);
  auto ret = bip::message_queue::remove(queueName_.c_str());
  if (ret == false) {
    LOG(ERROR) << "Failed to remove message queue=" << queueName_;
  }
  return ret;
}

AdminQueue::~AdminQueue() {

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
int AdminQueue::write(const GatewayMsg& gmsg) {
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
int AdminQueue::read(GatewayMsg& gmsg) {
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

int AdminQueue::try_read(GatewayMsg& gmsg) {
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

int AdminQueue::timed_read(GatewayMsg& gmsg, int millisec) {
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

void AdminQueue::updateStats(Statistics& which, size_t msgSize) {
  which.count_ ++;
  which.msgSize_ = msgSize;
}

std::string AdminQueue::getStats() const {
  std::ostringstream s;
  s 
    << "read={count=" << readStats_.count_ << ",msg_size=" << readStats_.msgSize_ << "}"
    << ",write={count=" << writeStats_.count_ << ",msg_size=" << writeStats_.msgSize_ << "}"
    ;
  return s.str();
}

void AdminQueue::clearStats() {
  readStats_.count_ = 0;
  readStats_.msgSize_.reset();
  writeStats_.count_ = 0;
  writeStats_.msgSize_.reset();
}

const std::string& AdminQueue::getName() const {
  return queueName_;
}
 
size_t AdminQueue::getCurrentQueueLen() const {
  if (!mq_) {
    throw std::runtime_error("invalid edgequeue");
  }
  return mq_->get_num_msg();
}

size_t AdminQueue::getMaxQueueLen() const {
  if (!mq_) {
    throw std::runtime_error("invalid edgequeue");
  }
  return mq_->get_max_msg();
}

size_t AdminQueue::getMaxMsgSize() const {
  if (!mq_) {
    throw std::runtime_error("invalid edgequeue");
  }
  return mq_->get_max_msg_size();
}

}
}
