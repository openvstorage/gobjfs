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

static std::string getResponseEdgeQueueName(int pid) {
  std::string str = "response_to_edge_pid_" + std::to_string(pid);
  return str;
}

static std::string getRequestEdgeQueueName(int pid) {
  std::string str = "request_from_edge_pid_" + std::to_string(pid);
  return str;
}

static std::string getHeapName(int pid) {
  std::string heapName = "shmem_for_edge_pid_" + std::to_string(pid);
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
  , maxAllocSize_(maxAllocSize) {

  isCreator_ = true;
  request_.maxMsgSize_ = maxMsgSize;
  response_.maxMsgSize_ = maxMsgSize;

  request_.queueName_ = getRequestEdgeQueueName(pid);
  response_.queueName_ = getResponseEdgeQueueName(pid);
  heapName_ = getHeapName(pid);

  //Should pre-existing message queue and shmem be removed ?
  //Not doing it to catch errors
  //remove(pid);

  try {
    response_.mq_ = gobjfs::make_unique<bip::message_queue>(bip::create_only, 
      response_.queueName_.c_str(),
      (BoostHeaderAdjustment + maxQueueLen),
      response_.maxMsgSize_);

    request_.mq_ = gobjfs::make_unique<bip::message_queue>(bip::create_only, 
      request_.queueName_.c_str(),
      (BoostHeaderAdjustment + maxQueueLen),
      request_.maxMsgSize_);
  
    segment_ = gobjfs::make_unique<bip::managed_shared_memory>(bip::create_only, 
      heapName_.c_str(),
      (BoostHeaderAdjustment + maxQueueLen) * maxAllocSize); 

    // preallocate all the required blocks from the segment
    for (size_t idx = 0; idx < maxQueueLen; idx ++) {
      cachedBlocks_.push_back(segment_->allocate(maxAllocSize));
    }

    // TODO : queue len should be total number of jobs in system
    LOG(INFO) << "created request queue=" << request_.queueName_ 
      << " response queue=" << response_.queueName_ 
      << ",shmem=" << heapName_ 
      << ",maxQueueLen=" << maxQueueLen
      << ",maxAllocSize=" << maxAllocSize
      << ",maxMsgSize=" << maxMsgSize;
  } catch (const std::exception& e) {

    response_.mq_.reset();
    request_.mq_.reset();
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

  request_.queueName_ = getRequestEdgeQueueName(pid);
  response_.queueName_ = getResponseEdgeQueueName(pid);
  heapName_ = getHeapName(pid);

  try {
    request_.mq_ = gobjfs::make_unique<bip::message_queue>(bip::open_only, request_.queueName_.c_str());
    response_.mq_ = gobjfs::make_unique<bip::message_queue>(bip::open_only, response_.queueName_.c_str());
    segment_ = gobjfs::make_unique<bip::managed_shared_memory>(bip::open_only, heapName_.c_str());
    request_.maxMsgSize_ = getRequestMaxMsgSize();
    response_.maxMsgSize_ = getResponseMaxMsgSize();
    // maxAllocSize_ = TODO otherwise getFreeMem fails for reader side
    LOG(INFO) 
      << "opened request queue=" << request_.queueName_ 
      << " response queue=" << response_.queueName_ 
      << ",shmem=" << heapName_ 
      << ",request_maxMsgSize=" << request_.maxMsgSize_
      << ",response_maxMsgSize=" << response_.maxMsgSize_;
  } catch (const std::exception& e) {

    request_.mq_.reset();
    response_.mq_.reset();
    segment_.reset();

    throw std::runtime_error(
        "failed to open edgequeue for pid=" + std::to_string(pid));
  }
}

int EdgeQueue::remove(int pid) {

  bip::message_queue::remove(getResponseEdgeQueueName(pid).c_str());
  bip::message_queue::remove(getRequestEdgeQueueName(pid).c_str());

  auto heapName_ = getHeapName(pid);
  bip::shared_memory_object::remove(heapName_.c_str());

  return 0;
}

EdgeQueue::~EdgeQueue() {

  if (isCreator_) {
    if (request_.mq_) {
      auto ret = bip::message_queue::remove(request_.queueName_.c_str());
      if (ret == false) {
        LOG(ERROR) << "Failed to remove message queue=" << request_.queueName_;
      }
    }
    if (response_.mq_) {
      auto ret = bip::message_queue::remove(response_.queueName_.c_str());
      if (ret == false) {
        LOG(ERROR) << "Failed to remove message queue=" << response_.queueName_;
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
int EdgeQueue::writeRequest(const GatewayMsg& gmsg) {
  try {
    auto sendStr = gmsg.pack();
    assert(sendStr.size() < request_.maxMsgSize_);
    request_.mq_->send(sendStr.c_str(), sendStr.size(), 0);
    updateStats(request_.writeStats_, sendStr.size());
    return 0;
  } catch (const std::exception& e) {
    return -1;
  }
}

/**
 * TODO : can throw
 */
int EdgeQueue::readRequest(GatewayMsg& msg) {
  uint32_t priority;
  size_t recvdSize;
  try {
    char buf[request_.maxMsgSize_];
    request_.mq_->receive(buf, request_.maxMsgSize_, recvdSize, priority);
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
      for (size_t idx = 0; idx < msg.numElems(); idx ++) {
        msg.rawbufVec_.push_back(segment_->get_address_from_handle(msg.bufVec_[idx]));
      }
    }
    updateStats(request_.readStats_, recvdSize);
    return 0;
  } catch (const std::exception& e) {
    return -1;
  }
}

/**
 * TODO : can throw
 */
int EdgeQueue::writeResponse(const GatewayMsg& gmsg) {
  try {
    auto sendStr = gmsg.pack();
    assert(sendStr.size() < response_.maxMsgSize_);
    response_.mq_->send(sendStr.c_str(), sendStr.size(), 0);
    updateStats(response_.writeStats_, sendStr.size());
    return 0;
  } catch (const std::exception& e) {
    return -1;
  }
}

/**
 * TODO : can throw
 */
int EdgeQueue::readResponse(GatewayMsg& msg) {
  uint32_t priority;
  size_t recvdSize;
  try {
    char buf[response_.maxMsgSize_];
    response_.mq_->receive(buf, response_.maxMsgSize_, recvdSize, priority);
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
      for (size_t idx = 0; idx < msg.numElems(); idx ++) {
        msg.rawbufVec_.push_back(segment_->get_address_from_handle(msg.bufVec_[idx]));
      }
    }
    updateStats(response_.readStats_, recvdSize);
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

std::vector<giocb*> EdgeQueue::giocb_from_GatewayMsg(const GatewayMsg& gmsg) {

  std::vector<giocb*> giocb_vec;
  giocb_vec.reserve(gmsg.numElems());

  for (size_t idx = 0; idx < gmsg.numElems(); idx ++) {
    giocb *iocb = new giocb;

    iocb->filename = gmsg.filenameVec_[idx];
    iocb->aio_offset = gmsg.offsetVec_[idx];
    iocb->aio_nbytes = gmsg.sizeVec_[idx];
    iocb->aio_buf = segment_->get_address_from_handle(gmsg.bufVec_[idx]);
    iocb->user_ctx = gmsg.edgePid_;

    giocb_vec.push_back(iocb);
  }

  return giocb_vec;
}

/**
 * called from RoraGateway to convert a read response
 * into a GatewayMsg 
 */
int EdgeQueue::GatewayMsg_from_giocb(GatewayMsg& gmsg, 
    const giocb& iocb, 
    ssize_t retval) {

  gmsg.opcode_ = Opcode::READ_RESP;
  gmsg.filenameVec_.push_back(iocb.filename);
  gmsg.offsetVec_.push_back(iocb.aio_offset);
  gmsg.sizeVec_.push_back(iocb.aio_nbytes);
  gmsg.bufVec_.push_back(segment_->get_handle_from_address(iocb.aio_buf));
  gmsg.retvalVec_.push_back(retval);
  gmsg.numElems_ ++;

  return 0;
}

void EdgeQueue::updateStats(Statistics& which, size_t msgSize) {
  which.count_ ++;
  which.msgSize_ = msgSize;
}

std::string EdgeQueue::getStats() const {
  std::ostringstream s;
  s 
    << "resp_read={count=" << response_.readStats_.count_ << ",msg_size=" << response_.readStats_.msgSize_ << "}"
    << ",resp_write={count=" << response_.writeStats_.count_ << ",msg_size=" << response_.writeStats_.msgSize_ << "}"
    << "req_read={count=" << response_.readStats_.count_ << ",msg_size=" << response_.readStats_.msgSize_ << "}"
    << ",req_write={count=" << response_.writeStats_.count_ << ",msg_size=" << response_.writeStats_.msgSize_ << "}"
    ;
  return s.str();
}

void EdgeQueue::clearStats() {
  response_.readStats_.count_ = 0;
  response_.readStats_.msgSize_.reset();
  response_.writeStats_.count_ = 0;
  response_.writeStats_.msgSize_.reset();

  request_.readStats_.count_ = 0;
  request_.readStats_.msgSize_.reset();
  request_.writeStats_.count_ = 0;
  request_.writeStats_.msgSize_.reset();
}

size_t EdgeQueue::getRequestCurrentQueueLen() const {
  if (!request_.mq_) {
    throw std::runtime_error("invalid edgequeue");
  }
  return request_.mq_->get_num_msg();
}

size_t EdgeQueue::getRequestMaxQueueLen() const {
  if (!request_.mq_) {
    throw std::runtime_error("invalid edgequeue");
  }
  return request_.mq_->get_max_msg();
}

size_t EdgeQueue::getRequestMaxMsgSize() const {
  if (!request_.mq_) {
    throw std::runtime_error("invalid edgequeue");
  }
  return request_.mq_->get_max_msg_size();
}

size_t EdgeQueue::getResponseCurrentQueueLen() const {
  if (!response_.mq_) {
    throw std::runtime_error("invalid edgequeue");
  }
  return response_.mq_->get_num_msg();
}

size_t EdgeQueue::getResponseMaxQueueLen() const {
  if (!response_.mq_) {
    throw std::runtime_error("invalid edgequeue");
  }
  return response_.mq_->get_max_msg();
}

size_t EdgeQueue::getResponseMaxMsgSize() const {
  if (!response_.mq_) {
    throw std::runtime_error("invalid edgequeue");
  }
  return response_.mq_->get_max_msg_size();
}

size_t EdgeQueue::getFreeMem() const {
  if (!segment_) {
    throw std::runtime_error("invalid edgequeue");
  }
  return (cachedBlocks_.size() * maxAllocSize_) + segment_->get_free_memory();
}

}
}
