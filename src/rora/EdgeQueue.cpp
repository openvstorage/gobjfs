#include <rora/EdgeQueue.h>
#include <rora/GatewayProtocol.h>
#include <gobjfs_client.h>

#include <string>
#include <gobjfs_log.h>
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

// allocate more segments than required to compensate
// for boost segment headers which take up space
// segment_->get_free_memory() != allocated 
static constexpr size_t BoostHeaderAdjustment = 5;

/**
 * this is a create
 */
EdgeQueue::EdgeQueue(int pid, 
    size_t maxQueueLen, 
    size_t maxMsgSize,
    size_t maxAllocSize) 
  : pid_(pid)
  , maxMsgSize_(maxMsgSize) {

  isCreator_ = true;

  queueName_ = getEdgeQueueName(pid);
  heapName_ = getHeapName(pid);

  //Should pre-existing message queue and shmem be removed ?
  //Not doing it to catch errors
  //remove(pid);

  try {
    mq_ = gobjfs::make_unique<bip::message_queue>(bip::create_only, 
      queueName_.c_str(),
      (BoostHeaderAdjustment + maxQueueLen),
      maxMsgSize_);
  
    segment_ = gobjfs::make_unique<bip::managed_shared_memory>(bip::create_only, 
      heapName_.c_str(),
      (BoostHeaderAdjustment + maxQueueLen) * maxAllocSize); 

    // preallocate all the required blocks from the segment
    for (size_t idx = 0; idx < maxQueueLen; idx ++) {
      cachedBlocks_.push_back(segment_->allocate(maxAllocSize));
    }

    // TODO : should be total number of jobs in system
    LOG(INFO) << "created edge queue=" << queueName_ 
      << ",shmem=" << heapName_ 
      << ",maxQueueLen=" << maxQueueLen
      << ",maxAllocSize=" << maxAllocSize
      << ",maxMsgSize=" << maxMsgSize_;
  } catch (const std::exception& e) {

    mq_.reset();
    segment_.reset();

    throw std::runtime_error(
        "failed to create edgequeue for pid=" + std::to_string(pid));

  }
}


/**
 * this is an open
 */
EdgeQueue::EdgeQueue(int pid) : pid_(pid) {

  isCreator_ =  false;

  queueName_ = getEdgeQueueName(pid);
  heapName_ = getHeapName(pid);

  try {
    mq_ = gobjfs::make_unique<bip::message_queue>(bip::open_only, queueName_.c_str());
    segment_ = gobjfs::make_unique<bip::managed_shared_memory>(bip::open_only, heapName_.c_str());
    maxMsgSize_ = getMaxMsgSize();
    LOG(INFO) << "opened edge queue=" << queueName_ 
      << ",shmem=" << heapName_ 
      << ",maxMsgSize=" << maxMsgSize_;
  } catch (const std::exception& e) {

    mq_.reset();
    segment_.reset();

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

  if (isCreator_) {
    if (mq_) {
      auto ret = bip::message_queue::remove(queueName_.c_str());
      if (ret == false) {
        LOG(ERROR) << "Failed to remove message queue=" << queueName_;
      }
    }
    if (segment_) {
      // deallocate all the preallocated blocks
      for (auto ptr : cachedBlocks_) {
        segment_->deallocate(ptr);
      }
      cachedBlocks_.clear();
      auto ret = bip::shared_memory_object::remove(heapName_.c_str());
      if (ret == false) {
        LOG(ERROR) << "Failed to remove shmem segment=" << heapName_;
      }
    }
  }
}

/**
 * TODO : can throw
 */
int EdgeQueue::write(const GatewayMsg& gmsg) {
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
 * TODO : can throw
 */
int EdgeQueue::read(GatewayMsg& msg) {
  uint32_t priority;
  size_t recvdSize;
  try {
    char buf[maxMsgSize_];
    mq_->receive(buf, maxMsgSize_, recvdSize, priority);
    msg.unpack(buf, recvdSize);
    if ((msg.opcode_ == Opcode::READ_REQ) ||
      (msg.opcode_ == Opcode::READ_RESP)) {

      // as it currently stands, the process which creates
      // the edge queue only gets read responses
      // while the rora gateway only gets read requests
      // encoded that check in this assert
      assert((isCreator_ && msg.opcode_ == Opcode::READ_RESP) ||
        (!isCreator_ && msg.opcode_ == Opcode::READ_REQ));

      // convert segment offset to raw ptr within this process
      msg.rawbuf_ = segment_->get_address_from_handle(msg.buf_);
    }
    updateStats(readStats_, recvdSize);
    return 0;
  } catch (const std::exception& e) {
    return -1;
  }
}
 
/**
 * Will be called only from EdgeProcess
 * Assume only one thread calls it
 * Therefore, Not thread-safe right now
 */
void* EdgeQueue::alloc(size_t sz) {
  void* retPtr = nullptr;
  if (!isCreator_) {
    LOG(ERROR) << "shared memory should only be alloc/freed by segment creator";
  } else {
    if (not cachedBlocks_.empty()) {
      // first check if any preallocated blocks exist
      // TODO : bzero the returned ptr ?
      retPtr = cachedBlocks_.front();
      cachedBlocks_.pop_front();
    } else {
      retPtr = segment_->allocate(sz);
    }
  }
  return retPtr;
}

/**
 * Will be called only from EdgeProcess
 * Assume only one thread calls it
 * Therefore, Not thread-safe right now
 */
int EdgeQueue::free(void* ptr) {
  if (!isCreator_) {
    LOG(ERROR) << "shared memory should only be alloc/freed by segment creator";
    return -EINVAL;
  }
  // keep the freed block in cached list
  //segment_->deallocate(ptr);
  cachedBlocks_.push_back(ptr);
  return 0;
}

giocb* EdgeQueue::giocb_from_GatewayMsg(const GatewayMsg& gmsg) {

  giocb *iocb = new giocb;

  iocb->filename = gmsg.filename_;
  iocb->aio_offset = gmsg.offset_;
  iocb->aio_nbytes = gmsg.size_;
  iocb->aio_buf = segment_->get_address_from_handle(gmsg.buf_);
  iocb->user_ctx = gmsg.edgePid_;

  return iocb;
}

/**
 * called from RoraGateway to convert a read response
 * into a GatewayMsg 
 */
int EdgeQueue::GatewayMsg_from_giocb(GatewayMsg& gmsg, 
    const giocb& iocb, 
    ssize_t retval, 
    int errval) {

  gmsg.opcode_ = Opcode::READ_RESP;
  gmsg.filename_ = iocb.filename;
  gmsg.offset_ = iocb.aio_offset;
  gmsg.size_ = iocb.aio_nbytes;
  gmsg.buf_ = segment_->get_handle_from_address(iocb.aio_buf);
  gmsg.errval_ = errval;
  gmsg.retval_ = retval;

  return 0;
}

void EdgeQueue::updateStats(Statistics& which, size_t msgSize) {
  which.count_ ++;
  which.msgSize_ = msgSize;
}

std::string EdgeQueue::getStats() const {
  std::ostringstream s;
  s 
    << "read={count=" << readStats_.count_ << ",msg_size=" << readStats_.msgSize_ << "}"
    << ",write={count=" << writeStats_.count_ << ",msg_size=" << writeStats_.msgSize_ << "}"
    ;
  return s.str();
}

void EdgeQueue::clearStats() {
  readStats_.count_ = 0;
  readStats_.msgSize_.reset();
  writeStats_.count_ = 0;
  writeStats_.msgSize_.reset();
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

}
}
