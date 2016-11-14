#pragma once

#include <boost/interprocess/ipc/message_queue.hpp>
#include <string>
#include <sys/stat.h> // mode_t
#include <util/Stats.h>

namespace bip = boost::interprocess;

namespace gobjfs {
namespace rora {

class GatewayMsg;

/**
 * The AdminQueue is one per machine/server
 * It processes following messages
 * 1) add_asd
 * 2) add_edge
 * 3) drop_edge
 * 4) drop_edge
 * 5) any other admin-type messages (add here)
 */
class AdminQueue {

  bool isCreator_{false};
  
  size_t maxMsgSize_{0};

  struct Statistics {
    // can have slight mismatch between various counters 
    // since updates are not atomic
    uint64_t count_{0};
    gobjfs::stats::StatsCounter<uint32_t> msgSize_;
  };

  std::string queueName_;
  std::unique_ptr<bip::message_queue> mq_{nullptr};
  Statistics readStats_;
  Statistics writeStats_;

  public:
  /**
   * create edge queue for version
   */
  AdminQueue(const std::string& version, size_t maxQueueLen);
      

  /**
   * open existing edge queue for version
   */
  AdminQueue(std::string version);

  static int remove(const std::string& version);

  ~AdminQueue();

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
  void updateStats(AdminQueue::Statistics& which, size_t msgSize);

  public:
  std::string getStats() const;

  void clearStats();
   
  size_t getCurrentQueueLen() const;

  size_t getMaxQueueLen() const;

  size_t getMaxMsgSize() const;

};

typedef std::unique_ptr<AdminQueue> AdminQueueUPtr;
typedef std::shared_ptr<AdminQueue> AdminQueueSPtr;

}
}
