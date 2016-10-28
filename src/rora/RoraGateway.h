#pragma once

#include <rora/EdgeQueue.h>
#include <rora/ASDQueue.h>
#include <util/EPoller.h>
#include <util/Stats.h>
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

    // polls for completed read requests from all asds
    gobjfs::os::EPoller epoller_;

    // thread listens for completed read requests across all 
    // client_ctx and sends the responses back to EdgeProcesses
    std::thread thread_;
    bool started_{false};
    bool stopping_{false};
    bool stopped_{false};

    public:

    int insert(EdgeQueueSPtr edgePtr);
    int drop(int pid);
    EdgeQueueSPtr find(int pid);

    // use kill(pid, 0) == ESRCH to check for dead pids to delete from catalog
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

    // per asdqueue stats
    struct Statistics {
      // how many requests were submitted in aio_readv
      gobjfs::stats::StatsCounter<uint64_t> submitBatchSize_;
      // how many responses were received together in callback
      gobjfs::stats::StatsCounter<uint64_t> callbackBatchSize_;
    }stats_;

    public:

    ASDInfo(const std::string& transport, const std::string& ipAddress, int port,
        size_t maxMsgSize, size_t maxQueueLen);

  };


  typedef std::unique_ptr<ASDInfo> ASDInfoUPtr;

  EdgeInfo edges_;
  std::list<ASDInfoUPtr> asdList_;


  int asdThreadFunc(ASDInfo* asdInfo);

  int responseThreadFunc();

  int handleReadCompletion(int fd, uintptr_t asdPtr);

  public:

  int init(const std::string &configFileName);

  int run();

  int shutdown();

};

}
}
