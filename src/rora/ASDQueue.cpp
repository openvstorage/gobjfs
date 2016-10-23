#include "ASDQueue.h"
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
    size_t maxMsgSize) {

  created_ = true;

  queueName_ = getASDQueueName(uri);

  //Should pre-existing message queue and shmem be removed ?
  //Not doing it to catch errors
  //remove(pid);

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
ssize_t ASDQueue::write(const char* buf, size_t sz) {
  try {
    mq_->send(buf, sz, 0);
    return sz;
  } catch (const std::exception& e) {
    return -1;
  }
}

/**
 * TODO : can throw
 */
ssize_t ASDQueue::read(char* buf, size_t sz) {
  uint32_t priority;
  size_t recvdSize;
  try {
    mq_->receive(buf, sz, recvdSize, priority);
    return recvdSize;
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
