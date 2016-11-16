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
    off_t offset_;
    size_t size_;
    void* buffer_;
};

struct eioRequest {
    std::vector<eiocb*> eiocbVec_;
    std::vector<ssize_t> retvalVec_;

    size_t size() const {
        return eiocbVec_.size();
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

  int addASD(std::string transport, std::string ipAddress, int port);

  // synchronous read
  int read(eioRequest& req);

  // async read
  int asyncRead(eioRequest& req);
  int release(eioRequest& req);
  int waitForResponse(eioRequest& req);

};

}}
