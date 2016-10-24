#pragma once

#include <rora/EdgeQueue.h>
#include <rora/ASDQueue.h>
#include <gobjfs_client.h>

#include <string>
#include <thread>
#include <mutex>
#include <map>

namespace gobjfs {
namespace rora {

class RoraGateway {

  private:

  struct EdgeInfo {
    // map from edge pid to edge queue
    std::map<int, EdgeQueueSPtr> catalog_;

    // mutex protects catalog insert/delete done by multiple asd threads
    std::mutex mutex_;

    // thread listens for completed read requests across all 
    // client_ctx and sends the responses back to EdgeProcesses
    std::thread thread_;

    public:

    int insert(EdgeQueueSPtr edgePtr);
    int drop(int pid);
    EdgeQueueSPtr find(int pid);
    int dropDeadEdgeProcesses();

  };

  struct ASDInfo {

    const std::string transport_;
    const std::string ipAddress_;
    int port_;

    ASDQueueUPtr queue_;

    // TODO for portals, there will be multiple ctx and asdThread_
    // how to configure number of threads for asd 
    gobjfs::xio::client_ctx_ptr ctx_;
    std::thread thread_;

    bool started_{false};
    bool stopping_{false};
    bool stopped_{false};

    public:

    ASDInfo(const std::string& transport, const std::string& ipAddress, int port,
        size_t maxMsgSize, size_t maxQueueLen);

  };

  typedef std::unique_ptr<ASDInfo> ASDInfoUPtr;

  EdgeInfo edges_;
  std::list<ASDInfoUPtr> asdList_;

  int asdFunc(ASDInfo* asdInfo);

  public:

  int init(const std::string &configFileName);

  int run();

  int shutdown();

};

}
}
