#pragma once

#include <rora/AdminQueue.h>
#include <rora/EdgeQueue.h>
#include <rora/ASDQueue.h>
#include <vector>
#include <string>

namespace gobjfs {
namespace rora {

struct eiocb;

struct eiocb {
    std::string filename_;
    off_t offset_{0};
    size_t size_{0};
    void* buffer_{nullptr}; // will be allocated internally
};

struct eioRequest {
    int32_t asdIdx_{-1};
    std::vector<eiocb*> eiocbVec_;
    std::vector<ssize_t> retvalVec_;

    size_t size() const {
        return eiocbVec_.size();
    }

    ~eioRequest() {
        for (auto iocb : eiocbVec_) {
            delete iocb;
        }
    }
};

struct GatewayClient {

  // version of rora gateway this client is connected to
  int32_t roraVersion_{-1};

  // shmem queue opened for sending admin msgs to rora gateway
  AdminQueueUPtr adminQueue_;

  // shmem queue created by this client to get responses
  EdgeQueueUPtr edgeQueue_;

  // asd queues opened to write read requests to rora gateway
  std::vector<ASDQueueUPtr> asdQueueVec_;

  GatewayClient(int32_t roraVersion,
      size_t maxOutstandingIO,
      size_t blockSize);

  int shutdown();

  ~GatewayClient();

  /*
   * @return asdIdx which has to be set in suAsequent eiocb requests
   */
  int32_t addASD(std::string transport, std::string ipAddress, int port);

  // synchronous read
  int read(eioRequest& req);

  // async read
  int asyncRead(eioRequest& req);
  int release(eioRequest& req);
  int waitForResponse(eioRequest& req);

};

}}
