#pragma once

#include <rora/AdminQueue.h>
#include <rora/EdgeQueue.h>
#include <rora/ASDQueue.h>
#include <vector>
#include <string>

namespace gobjfs {
namespace rora {

struct EdgeIOCB {
    std::string filename_;
    off_t offset_{0};
    size_t size_{0};
    void* buffer_{nullptr}; // will be allocated internally
};

struct EdgeIORequest {
    int32_t asdIdx_{-1};
    std::vector<std::unique_ptr<EdgeIOCB>> eiocbVec_;
    std::vector<ssize_t> retvalVec_;

    void append(EdgeIORequest& other);
    size_t size() const {
        return eiocbVec_.size();
    }
};

/**
 * Client is NOT thread-safe
 */
class GatewayClient {

  // version of rora gateway this client is connected to
  int32_t roraVersion_{-1};

  // shmem queue opened for sending admin msgs to rora gateway
  AdminQueueUPtr adminQueue_;

  // shmem queue created by this client to get responses
  EdgeQueueUPtr edgeQueue_;

  // map of ASDQueues opened to write read requests to rora gateway
  typedef int32_t ASDId;
  std::vector<ASDQueueUPtr> asdQueueVec_;


  struct Statistics {
      // preliminary - expand more
      uint64_t numSubmitted_{0};
      uint64_t numCompleted_{0};
  };
  std::map<ASDId, Statistics> stats_;

public:

  GatewayClient(int32_t roraVersion,
      size_t maxOutstandingIO,
      size_t blockSize);

  // call shutdown if u are in hurry and don't want to wait for destructor
  int shutdown();

  ~GatewayClient();

  /*
   * @return on success, returns the asdIdx to be passed in subsequent eiocb requests
   *         on failure, returns negative error code
   */
  int32_t addASD(std::string transport, std::string ipAddress, int port);

  std::string getStats();

  // synchronous read
  int read(EdgeIORequest& req);

  // async read
  int asyncRead(EdgeIORequest& req);

  // wait for a response
  // TODO : fill the asdIdx in GatewayMsg so we can read it back here
  int waitForResponse(EdgeIORequest& req);

  // release shared memory associated with request
  int release(EdgeIORequest& req);

};

}}
