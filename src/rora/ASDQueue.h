#pragma once

#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <string>
#include <sys/stat.h> // mode_t
#include <util/Stats.h>

namespace bip = boost::interprocess;

namespace gobjfs {
namespace rora {

class GatewayMsg;

/**
 * An ASDQueue abstracts the message queue and 
 * required for receiving read requests on the Gateway
 */
class ASDQueue {

  public:
  int32_t version_;
  std::string transport_;
  std::string ipAddress_;
  int port_;
  std::string queueName_;

  std::unique_ptr<bip::message_queue> mq_{nullptr};
  
  bool isCreator_{false};

  size_t maxMsgSize_{0};

  struct Statistics {
    // can have slight mismatch between various counters 
    // since updates are not atomic
    uint64_t count_{0};
    gobjfs::stats::StatsCounter<uint32_t> msgSize_;
  };

  Statistics readStats_;
  Statistics writeStats_;

  public:
  /**
   * create edge queue for uri
   */
  explicit ASDQueue(int32_t version,
      std::string transport, std::string ipAddress, int port,
      size_t maxQueueLen, size_t maxMsgSize);
      

  /**
   * open existing edge queue for uri
   */
  explicit ASDQueue(int32_t version,
      std::string transport, std::string ipAddress, int port);

  static int remove(int32_t version,
      std::string ipAddress, int port);

  ~ASDQueue();

  int write(const GatewayMsg& gmsg);

  int read(GatewayMsg& gmsg);

  /**
   * @return -EAGAIN if got nothing 
   */
  int try_read(GatewayMsg& gmsg);

  /**
   * @param millisec wait before returning
   * @return -EAGAIN if got nothing 
   */
  int timed_read(GatewayMsg& gmsg, int millisec);

  const std::string& getName() const;

  private:
  void updateStats(ASDQueue::Statistics& which, size_t msgSize);

  public:
  std::string getStats() const;

  void clearStats();
   
  size_t getCurrentQueueLen() const;

  size_t getMaxQueueLen() const;

  size_t getMaxMsgSize() const;

};

typedef std::unique_ptr<ASDQueue> ASDQueueUPtr;
typedef std::shared_ptr<ASDQueue> ASDQueueSPtr;

}
}
