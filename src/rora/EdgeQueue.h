#pragma once

#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <string>
#include <sys/stat.h> // mode_t

namespace bip = boost::interprocess;

namespace gobjfs {
namespace rora {

/**
 * An EdgeQueue combines the message queue and 
 * shared memory segment required for processing read requests.
 *
 * The read response will be sent over the message queue
 * The pointer to the buffer which was read will be in the
 * shared memory segment
 */
class EdgeQueue {

  std::string queueName_;
  std::string heapName_;

  bip::message_queue *mq_{nullptr};
  bip::managed_shared_memory *segment_{nullptr};
  
  bool created_{false};

  public:
  /**
   * create edge queue for pid
   */
  EdgeQueue(int pid, size_t maxQueueLen, size_t maxMsgSize,
      size_t maxAllocSize);

  /**
   * open existing edge queue for pid
   */
  EdgeQueue(int pid);

  static int remove(int pid);

  ~EdgeQueue();

  ssize_t write(char* buf, size_t sz);

  ssize_t read(char* buf, size_t sz);
   
  void* alloc(size_t sz);

  int free(void* ptr);

  size_t getCurrentQueueLen() const;

  size_t getMaxQueueLen() const;

  size_t getMaxMsgSize() const;

  size_t getFreeMem() const;
};

}
}
