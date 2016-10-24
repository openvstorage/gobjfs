#pragma once

#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <string>
#include <sys/stat.h> // mode_t

namespace bip = boost::interprocess;

// forward delaration 
namespace gobjfs {
  namespace xio {
    struct giocb;
  }
}

namespace gobjfs {
namespace rora {

class GatewayMsg;


/**
 * An EdgeQueue combines the message queue and 
 * shared memory segment required for processing read requests.
 *
 * The read response will be sent over the message queue
 * The pointer to the buffer which was read will be in the
 * shared memory segment
 */
class EdgeQueue {

  public:
  int pid_{-1};

  private:
  std::string queueName_;
  std::string heapName_;
  bool created_{false};

  size_t maxMsgSize_{0};

  public:

  std::unique_ptr<bip::message_queue> mq_{nullptr};
  std::unique_ptr<bip::managed_shared_memory> segment_{nullptr};

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

  int write(const GatewayMsg& gmsg);

  int read(GatewayMsg& gmsg);
   
  void* alloc(size_t sz);

  int free(void* ptr);

  int giocb_from_GatewayMsg(gobjfs::xio::giocb& iocb, const GatewayMsg& gmsg);

  /**
   * @param retval: actual size which was read on server, or -1 in failure
   * @param errval: value of errno in case retval is -1, else 0
   */
  int GatewayMsg_from_giocb(GatewayMsg& gmsg, const gobjfs::xio::giocb& iocb,
    ssize_t retval, 
    int errval);

  size_t getCurrentQueueLen() const;

  size_t getMaxQueueLen() const;

  size_t getMaxMsgSize() const;

  size_t getFreeMem() const;
};

typedef std::unique_ptr<EdgeQueue> EdgeQueueUPtr;
typedef std::shared_ptr<EdgeQueue> EdgeQueueSPtr;

}
}
