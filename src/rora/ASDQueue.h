#pragma once

#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <string>
#include <sys/stat.h> // mode_t

namespace bip = boost::interprocess;

namespace gobjfs {
namespace rora {

class GatewayMsg;

/**
 * An ASDQueue abstracts the message queue and 
 * required for receiving read requests on the Gateway
 */
class ASDQueue {

  std::string queueName_;

  std::unique_ptr<bip::message_queue> mq_{nullptr};
  
  bool created_{false};

  size_t maxMsgSize_{0};

  public:
  /**
   * create edge queue for uri
   */
  ASDQueue(const std::string& uri, size_t maxQueueLen, size_t maxMsgSize);
      

  /**
   * open existing edge queue for uri
   */
  ASDQueue(const std::string& uri);

  static int remove(const std::string& uri);

  ~ASDQueue();

  int write(const GatewayMsg& gmsg);

  int read(GatewayMsg& gmsg);
   
  size_t getCurrentQueueLen() const;

  size_t getMaxQueueLen() const;

  size_t getMaxMsgSize() const;

};

typedef std::unique_ptr<ASDQueue> ASDQueueUPtr;
typedef std::shared_ptr<ASDQueue> ASDQueueSPtr;

}
}
