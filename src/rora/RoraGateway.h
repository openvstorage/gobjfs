#pragma once

#include <rora/EdgeQueue.h>
#include <rora/ASDQueue.h>
#include <rora/AdminQueue.h>
#include <util/EPoller.h>
#include <util/Stats.h>
#include <util/TimerNotifier.h>
#include <gobjfs_client.h>

#include <string>
#include <thread>
#include <vector>
#include <set>
#include <mutex>
#include <map>

namespace gobjfs {
namespace rora {

class RoraGateway {

  private:

  static const int watchDogTimeSec_;

  struct Config {
  
    size_t maxQueueLen_ {256};
    // client threads to connect to multiple portals
    size_t maxEPollerThreads_{1}; 
    size_t maxThreadsPerASD_{1}; 
    size_t maxConnPerASD_{1}; 

    int readConfig(const std::string& configFileName);
  };

  // per thread info
  struct ThreadInfo {
    std::thread thread_;
    int threadId_; // gettid
    bool started_{false};
    bool stopping_{false};
    bool stopped_{false};
  };

  size_t maxEPollerThreads_{1};

  struct EdgeInfo {
    // map from edge pid to edge queue
    std::map<int, EdgeQueueSPtr> catalog_;

    // mutex protects catalog insert/delete done by multiple asd threads
    mutable std::mutex mutex_;

    // polls for completed read requests from all asds
    gobjfs::os::EPoller epoller_;

    // single thread listens for completed read requests across all ASD
    // client_ctx and sends the responses back to EdgeProcesses
    std::vector<std::shared_ptr<ThreadInfo>> epollerThreadsVec_;

    public:

    int insert(EdgeQueueSPtr edgePtr);
    int drop(int pid);
    EdgeQueueSPtr find(int pid);
    size_t size() const;

    int cleanupForDeadEdgeProcesses();

  };

  class ASDInfo;

  /**
   * variables used in thread loop are also part of this structure.
   * just examine this struct during debugging from any thread, 
   * instead of switching threads and examing local variables
   */
  struct ASDThreadInfo : public ThreadInfo {
    std::vector<gobjfs::xio::giocb*> pending_giocb_vec_;
    int timeout_ms_ = 1;
    int numIdleLoops_ = 0;
    uint32_t numLoops_ = 0;
    size_t nextConnIdx_ = 0;

    /**
     * thread which forwards read requests to ASD
     */
    int threadFunc(RoraGateway* rgPtr, ASDInfo* asdInfo, size_t thrIdx);
  };

  struct ASDInfo {

    const std::string transport_;
    const std::string ipAddress_;
    int port_{-1};

    size_t maxThreadsPerASD_{0};
    size_t maxConnPerASD_{0};

    ASDQueueUPtr queue_;

    std::vector<gobjfs::xio::client_ctx_ptr> ctxVec_;
    std::vector<std::shared_ptr<ASDThreadInfo>> asdThreadVec_;

    std::set<int> edgesUsingMe_;

    // info passed to EPoller.addEvent
    // to figure out the fd corresponding to the ctx
    struct CallbackInfo {
      ASDInfo* asdPtr_{nullptr};
      int connIdx_{-1};
    };

    // keep list of shared_ptr which will be destroyed automatically
    // on rora gateway exit
    std::list<std::shared_ptr<CallbackInfo>> callbackInfoList_;

    // per asdqueue stats
    struct Statistics {
      // how many requests were submitted in aio_readv
      // TODO : many threads can update this variable
      gobjfs::stats::StatsCounter<uint64_t> submitBatchSize_;
      // how many responses were received together in callback
      gobjfs::stats::StatsCounter<uint64_t> callbackBatchSize_;
    }stats_;

    public:

    ASDInfo(RoraGateway* rgPtr,
        const std::string& transport, const std::string& ipAddress, int port,
        size_t maxMsgSize, size_t maxQueueLen,
        size_t maxThreadsPerASD,
        size_t maxConnPerASD);

    void clearStats();
  };

  typedef std::unique_ptr<ASDInfo> ASDInfoUPtr;

  Config config_;

  // queue for receiving add/drop of ASD/Edge requests
  AdminQueueUPtr adminQueuePtr_;
  ThreadInfo adminThread_;

  // catalog of registered edges
  EdgeInfo edges_;
  // list of ASDs
  std::vector<ASDInfoUPtr> asdVec_;
  // watchdog timer
  std::unique_ptr<gobjfs::os::TimerNotifier> watchDogPtr_;

  /**
   * thread which processes admin messages
   */
  int adminThreadFunc();

  /**
   * thread which transmits read responses back to edges
   */
  int responseThreadFunc(size_t thrIdx);

  /**
   * handler for reading xio ctx queue 
   * it is called from epoller
   */
  int handleReadCompletion(int fd, uintptr_t asdPtr);

  /**
   * handler for cleaning up queues
   * it is called from epoller
   */
  int watchDogFunc(int fd, uintptr_t userCtx);

  public:

  int init(const std::string &configFileName);

  int addASD(const std::string& transport,
    const std::string& ipAddress,
    const int port,
    int edgePid);

  int dropASD(const std::string& transport,
    const std::string& ipAddress,
    const int port,
    int edgePid);

  int run();

  int shutdown();

};

}
}
