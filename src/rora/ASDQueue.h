#pragma once

#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <string>
#include <sys/stat.h> // mode_t

namespace bip = boost::interprocess;

namespace gobjfs {
namespace rora {

/**
 * An ASDQueue abstracts the message queue and 
 * required for receiving read requests on the Gateway
 */
class ASDQueue {

  std::string queueName_;

  bip::message_queue *mq_{nullptr};
  
  bool created_{false};

  public:
  /**
   * create edge queue for pid
   */
  ASDQueue(const std::string& uri, size_t maxQueueLen, size_t maxMsgSize);
      

  /**
   * open existing edge queue for pid
   */
  ASDQueue(const std::string& uri);

  static int remove(const std::string& uri);

  ~ASDQueue();

  ssize_t write(const char* buf, size_t sz);

  ssize_t read(char* buf, size_t sz);
   
  size_t getCurrentQueueLen() const;

  size_t getMaxQueueLen() const;

  size_t getMaxMsgSize() const;

};

}
}
