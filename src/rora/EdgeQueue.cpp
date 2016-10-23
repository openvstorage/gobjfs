#include "EdgeQueue.h"
#include <string>
#include <glog/logging.h>
#include <type_traits>

namespace bip = boost::interprocess;

namespace gobjfs {
namespace rora {

static std::string getEdgeQueueName(int pid) {
  std::string str = "respqueue_to_pid_" + std::to_string(pid);
  return str;
}

static std::string getHeapName(int pid) {
  std::string heapName = "shmem_for_pid_" + std::to_string(pid);
  return heapName;
}

/**
 * this is a create
 */
EdgeQueue::EdgeQueue(int pid, 
    size_t maxQueueLen, 
    size_t maxMsgSize,
    size_t maxAllocSize) {

  created_ = true;

  queueName_ = getEdgeQueueName(pid);
  heapName_ = getHeapName(pid);

  //Should pre-existing message queue and shmem be removed ?
  //Not doing it to catch errors
  //remove(pid);

  try {
    mq_ = new bip::message_queue(bip::create_only, 
      queueName_.c_str(),
      maxQueueLen,
      maxMsgSize);
  
    segment_ = new bip::managed_shared_memory(bip::create_only, 
      heapName_.c_str(),
      maxQueueLen * maxAllocSize); 
    // TODO : should be total number of jobs in system
  } catch (const std::exception& e) {

    delete mq_;
    mq_ = nullptr;

    delete segment_;
    segment_ = nullptr;

    throw std::runtime_error(
        "failed to create edgequeue for pid=" + std::to_string(pid));

  }
}


/**
 * this is an open
 */
EdgeQueue::EdgeQueue(int pid) {

  created_ =  false;

  queueName_ = getEdgeQueueName(pid);
  heapName_ = getHeapName(pid);

  try {
    mq_ = new bip::message_queue(bip::open_only, queueName_.c_str());
    segment_ = new bip::managed_shared_memory(bip::open_only, heapName_.c_str());
  } catch (const std::exception& e) {

    delete mq_;
    mq_ = nullptr;

    delete segment_;
    segment_ = nullptr;

    throw std::runtime_error(
        "failed to open edgequeue for pid=" + std::to_string(pid));
  }
}

int EdgeQueue::remove(int pid) {

  auto queueName_ = getEdgeQueueName(pid);
  bip::message_queue::remove(queueName_.c_str());

  auto heapName_ = getHeapName(pid);
  bip::shared_memory_object::remove(heapName_.c_str());

  return 0;
}

EdgeQueue::~EdgeQueue() {

  delete segment_;
  delete mq_;

  if (created_) {
    if (mq_) {
      auto ret = bip::message_queue::remove(queueName_.c_str());
      if (ret == false) {
        LOG(ERROR) << "Failed to remove message queue=" << queueName_;
      }
    }
    if (segment_) {
      auto ret = bip::shared_memory_object::remove(heapName_.c_str());
      if (ret == false) {
        LOG(ERROR) << "Failed to remove shmem segment=" << heapName_;
      }
    }
  }

  segment_ = nullptr;
  mq_ = nullptr;
}

/**
 * TODO : can throw
 */
ssize_t EdgeQueue::write(char* buf, size_t sz) {
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
ssize_t EdgeQueue::read(char* buf, size_t sz) {
  uint32_t priority;
  size_t recvdSize;
  try {
    mq_->receive(buf, sz, recvdSize, priority);
    return recvdSize;
  } catch (const std::exception& e) {
    return -1;
  }
}
 
void* EdgeQueue::alloc(size_t sz) {
  if (!created_) {
    LOG(ERROR) << "shared memory should only be alloc/freed by segment creator";
    return nullptr;
  }
  return segment_->allocate(sz);
}

int EdgeQueue::free(void* ptr) {
  if (!created_) {
    LOG(ERROR) << "shared memory should only be alloc/freed by segment creator";
    return -EINVAL;
  }
  segment_->deallocate(ptr);
  return 0;
}

size_t EdgeQueue::getCurrentQueueLen() const {
  if (!mq_) {
    throw std::runtime_error("invalid edgequeue");
  }
  return mq_->get_num_msg();
}

size_t EdgeQueue::getMaxQueueLen() const {
  if (!mq_) {
    throw std::runtime_error("invalid edgequeue");
  }
  return mq_->get_max_msg();
}

size_t EdgeQueue::getMaxMsgSize() const {
  if (!mq_) {
    throw std::runtime_error("invalid edgequeue");
  }
  return mq_->get_max_msg_size();
}

size_t EdgeQueue::getFreeMem() const {
  if (!segment_) {
    throw std::runtime_error("invalid edgequeue");
  }
  auto p = segment_->get_segment_manager();
  return segment_->get_free_memory();
}

//size_t EdgeQueue::

}
}
