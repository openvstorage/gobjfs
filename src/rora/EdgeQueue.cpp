#include <rora/EdgeQueue.h>
#include <rora/GatewayProtocol.h>
#include <gobjfs_client.h>

#include <string>
#include <glog/logging.h>
#include <type_traits>

namespace bip = boost::interprocess;

using gobjfs::xio::giocb;

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
    size_t maxAllocSize) 
  : maxMsgSize_(maxMsgSize) {

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
      maxMsgSize_);
  
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
    maxMsgSize_ = getMaxMsgSize();
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
int EdgeQueue::write(const GatewayMsg& gmsg) {
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
int EdgeQueue::read(GatewayMsg& msg) {
  uint32_t priority;
  size_t recvdSize;
  try {
    char buf[maxMsgSize_];
    mq_->receive(buf, maxMsgSize_, recvdSize, priority);
    msg.unpack(buf, recvdSize);
    return 0;
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

int EdgeQueue::giocb_from_GatewayMsg(giocb& iocb, const GatewayMsg& gmsg) {

  iocb.filename = gmsg.filename_;
  iocb.aio_offset = gmsg.offset_;
  iocb.aio_nbytes = gmsg.size_;
  iocb.aio_buf = segment_->get_address_from_handle(gmsg.buf_);
  iocb.user_ctx = gmsg.edgePid_;

  return 0;
}

int EdgeQueue::GatewayMsg_from_giocb(GatewayMsg& gmsg, 
    const giocb& iocb, 
    ssize_t retval, 
    int errval) {

  gmsg.opcode_ = Opcode::READ;
  gmsg.filename_ = iocb.filename;
  gmsg.offset_ = iocb.aio_offset;
  gmsg.size_ = iocb.aio_nbytes;
  gmsg.buf_ = segment_->get_handle_from_address(iocb.aio_buf);
  gmsg.errval_ = errval;
  gmsg.retval_ = retval;

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
  return segment_->get_free_memory();
}

//size_t EdgeQueue::

}
}
