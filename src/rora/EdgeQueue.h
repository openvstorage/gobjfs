#pragma once

#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <string>
#include <vector>
#include <deque>
#include <sys/stat.h> // mode_t
#include <util/Stats.h> // mode_t

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
  size_t maxAllocSize_{0};

  private:
  std::string heapName_;
  bool isCreator_{false};


  // allocated cached blocks
  std::deque<void*> cachedBlocks_;

  struct Statistics {
    // can have slight mismatch between various counters 
    // since updates are not atomic
    uint64_t count_{0};
    gobjfs::stats::StatsCounter<uint32_t> msgSize_;
  };

  struct Queue {
    Statistics readStats_;
    Statistics writeStats_;
    size_t maxMsgSize_{0};
    std::string queueName_;
    std::unique_ptr<bip::message_queue> mq_{nullptr};
  };

  public:

  Queue response_;
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

  int writeRequest(const GatewayMsg& gmsg);

  int readRequest(GatewayMsg& gmsg);

  int tryReadRequest(GatewayMsg& gmsg);

  int timedReadRequest(GatewayMsg& gmsg, int millisec);

  int writeResponse(const GatewayMsg& gmsg);

  int readResponse(GatewayMsg& gmsg);
   
  void* alloc(size_t sz);

  int free(void* ptr);

  std::vector<gobjfs::xio::giocb*> giocb_from_GatewayMsg(const GatewayMsg& gmsg);

  /**
   * @param retval: actual size which was read on server, or -errno in failure
   */
  int GatewayMsg_from_giocb(GatewayMsg& gmsg, const gobjfs::xio::giocb& iocb,
    ssize_t retval);

  private:
  void updateStats(EdgeQueue::Statistics& which, size_t msgSize);

  public:
  std::string getStats() const;

  void clearStats();

  size_t getResponseCurrentQueueLen() const;

  size_t getResponseMaxQueueLen() const;

  size_t getResponseMaxMsgSize() const;

  size_t getRequestCurrentQueueLen() const;

  size_t getRequestMaxQueueLen() const;

  size_t getRequestMaxMsgSize() const;

  size_t getFreeMem() const;
};

typedef std::unique_ptr<EdgeQueue> EdgeQueueUPtr;
typedef std::shared_ptr<EdgeQueue> EdgeQueueSPtr;

}
}
