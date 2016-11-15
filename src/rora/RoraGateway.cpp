
#include "RoraGateway.h"
#include <rora/GatewayProtocol.h>

#include <boost/program_options.hpp>
#include <gobjfs_log.h>
#include <util/lang_utils.h>
#include <util/os_utils.h>
#include <signal.h>

namespace bpo = boost::program_options;
using namespace gobjfs::xio;
using gobjfs::os::EPoller;
using gobjfs::os::TimerNotifier;

namespace gobjfs {
namespace rora {

int RoraGateway::Config::readConfig(const std::string& configFileName, 
    int argc, const char* argv[]) {

  bpo::options_description generic("options allowed in config file and command line(command line overrides)");
  generic.add_options()
      ("max_asd_queue", bpo::value<size_t>(&maxASDQueueLen_)->default_value(1024), "length of shared memory queue used to forward requests to ASD")
      ("max_admin_queue", bpo::value<size_t>(&maxAdminQueueLen_)->default_value(10), "length of shared memory queue used for admin requests")
      ("max_threads_per_asd", bpo::value<size_t>(&maxThreadsPerASD_)->default_value(8), "max threads which service all connections")
      ("max_conn_per_asd", bpo::value<size_t>(&maxConnPerASD_)->default_value(8), "max connections to an ASD (exploit accelio portals)")
      ("max_epoller_threads", bpo::value<size_t>(&maxEPollerThreads_)->default_value(4), "max threads for epoll")
      ("watchdog_time_sec", bpo::value<size_t>(&watchDogTimeSec_)->default_value(30), "interval for watchdog timer")
      ("daemon", bpo::value<bool>(&isDaemon_)->default_value(false), "run as daemon")
      ("version", bpo::value<int32_t>(&version_)->default_value(1), "change version of Rora Gateway to run multiple versions on same machine.  This version field gets embedded in names of shared mem queues")
      ;

  bpo::options_description cmdline("command line options");
  cmdline.add_options()
    ("help", "produce help message")
    ;

  cmdline.add(generic);

  std::ifstream configFile(configFileName);
  bpo::variables_map vm;
  // parse cmd line parameters first, because the value stored first 
  // into map overrides later updates (also see "composing" in boost)
  bpo::store(bpo::parse_command_line(argc, argv, cmdline), vm);
  bpo::store(bpo::parse_config_file(configFile, generic), vm);
  bpo::notify(vm);

  // TODO check values not too high or low
  if (vm.count("help")) {
    std::cout << cmdline << std::endl;
    return 1;
  } 

  LOG(INFO)
      << "================================================================="
      << std::endl << "config read is " << std::endl;

  std::ostringstream s;
  for (const auto &it : vm) {
    s << it.first.c_str() << "=";
    auto &value = it.second.value();
    if (auto v = boost::any_cast<std::vector<int>>(&value)) {
      for (auto val : *v) {
        s << val << ",";
      }
      s << std::endl;
    } else if (auto v = boost::any_cast<std::vector<std::string>>(&value)) {
      for (auto val : *v) {
        s << val << ",";
      }
      s << std::endl;
    } else if (auto v = boost::any_cast<int32_t>(&value)) {
      s << *v << std::endl;
    } else if (auto v = boost::any_cast<bool>(&value)) {
      s << *v << std::endl;
    } else if (auto v = boost::any_cast<size_t>(&value)) {
      s << *v << std::endl;
    } else {
      s << "cannot interpret value " << std::endl;
    }
  }

  LOG(INFO) << s.str();
  LOG(INFO)
      << "=================================================================="
      << std::endl;
  return 0;
}

//
// ============== ASDINFO ===========================

RoraGateway::ASDInfo::ASDInfo(RoraGateway* rgPtr,
    const std::string &transport, 
    const std::string &ipAddress, 
    int port,
    size_t maxMsgSize,
    size_t maxQueueLen,
    size_t maxThreadsPerASD,
    size_t maxConnPerASD) 
  : transport_(transport)
  , ipAddress_(ipAddress)
  , port_(port) 
  , maxThreadsPerASD_(maxThreadsPerASD)
  , maxConnPerASD_(maxConnPerASD) {

  int ret = 0;

  queue_ = gobjfs::make_unique<ASDQueue>(rgPtr->version_,
      transport_, ipAddress_, port_, maxQueueLen, maxMsgSize);

  auto ctx_attr_ptr = ctx_attr_new();
  ctx_attr_set_transport(ctx_attr_ptr, transport, ipAddress, port);

  // check maxConn = integer multiple of maxThreads
  // so that each thread can service connections by
  // arithmetic progression
  if (maxConnPerASD_ % maxThreadsPerASD_ != 0) {
    maxConnPerASD_ -= (maxConnPerASD_ % maxThreadsPerASD_);
  }

  LOG(INFO) << "uri=" << ipAddress_ << ":" << port_
    << ",maxConn=" << maxConnPerASD_
    << ",maxThreads=" << maxThreadsPerASD_;

  ctxVec_.reserve(maxConnPerASD_);
  asdThreadVec_.reserve(maxThreadsPerASD_);
  // Many threads can read from one shmem queue and 
  // forward requests to the server independently
  for (size_t idx = 0; idx < maxConnPerASD_; idx ++) {
    auto ctx = ctx_new(ctx_attr_ptr);
    assert(ctx);
    int err = ctx_init(ctx, maxQueueLen);
    assert(err == 0);

    ctxVec_.push_back(ctx);

    // for each ASD connection, add an event to monitor for read completions
    auto callbackCtx = std::make_shared<ASDInfo::CallbackInfo>();
    callbackCtx->asdPtr_ = this;
    callbackCtx->connIdx_ = idx;
    const int fd_to_add = aio_geteventfd(ctx);

    ret = rgPtr->edges_.epoller_.addEvent(reinterpret_cast<uintptr_t>(callbackCtx.get()), 
      fd_to_add,
      EPOLLIN, 
      std::bind(&RoraGateway::handleReadCompletion, rgPtr,
        std::placeholders::_1,
        std::placeholders::_2));

    if (ret != 0) {
      LOG(ERROR) << "failed to add fd=" << fd_to_add 
        << " for asd uri=" << ipAddress << ":" << port 
        << " conn_index=" << idx;
    } else {
      // save callback info in a shared_ptr list so it doesnt get destroyed
      callbackInfoList_.push_back(callbackCtx);
    }
  }

  // TODO clean everything & throw if ret != 0
}

int RoraGateway::ASDInfo::destroyAll(RoraGateway* rgPtr) {


    // shut down threads
    for (auto threadInfo : asdThreadVec_) {
      threadInfo->stopping_ = true;
      threadInfo->thread_.join();
    }
    asdThreadVec_.clear();

    // drop the epoller event 
    for (auto& callbackCtx : callbackInfoList_) { 
      int dropRet = rgPtr->edges_.epoller_.dropEvent(reinterpret_cast<uintptr_t>(callbackCtx.get()), 
        aio_geteventfd(ctxVec_[callbackCtx->connIdx_]));
      if (dropRet != 0) {
        LOG(ERROR) << "failed to delete fd"
          << " for asd uri=" << ipAddress_ << ":" << port_
          << " conn_index=" << callbackCtx->connIdx_;
      }
    }
    callbackInfoList_.clear();

    // shut down connections
    for (auto& ctx : ctxVec_) {
      ctx.reset();
    }
    ctxVec_.clear();
    // shut down the queue
    queue_.reset();

    // clear the edges set
    edgesUsingMe_.clear();

    return 0;
}

void RoraGateway::ASDInfo::clearStats() {
  stats_.submitBatchSize_.reset();
  stats_.callbackBatchSize_.reset();
  queue_->clearStats();
}

// ============== EDGEINFO ===========================

EdgeQueueSPtr RoraGateway::EdgeInfo::find(int pid) {
  std::unique_lock<std::mutex> l(edgeRegistryMutex_);
  auto iter = edgeRegistry_.find(pid);
  if (iter != edgeRegistry_.end()) {
    return iter->second;
  }
  return nullptr;
}

int RoraGateway::EdgeInfo::drop(int pid) {
  std::unique_lock<std::mutex> l(edgeRegistryMutex_);
  size_t sz = edgeRegistry_.erase(pid);
  if (sz != 1) {
    LOG(ERROR) << "Failed to find edge entry for pid=" << pid;
    return -1;
  }
  return 0;
}

size_t RoraGateway::EdgeInfo::size() const {
  std::unique_lock<std::mutex> l(edgeRegistryMutex_);
  return edgeRegistry_.size();
}

int RoraGateway::EdgeInfo::insert(EdgeQueueSPtr edgePtr) {
  std::unique_lock<std::mutex> l(edgeRegistryMutex_);
  edgeRegistry_.insert(std::make_pair(edgePtr->pid_, edgePtr));
  return 0;
}

int RoraGateway::EdgeInfo::cleanupForDeadEdgeProcesses() {
  std::unique_lock<std::mutex> l(edgeRegistryMutex_, std::try_to_lock);

  if (!l.owns_lock()) {
    LOG(WARNING) << "failed to obtain edge catalog mutex.  returning";
    return 0;
  }
  if (!edgeRegistry_.size()) {
    return 0;
  }
  for (auto& edgeIter : edgeRegistry_) {
    if (kill(edgeIter.second->pid_, 0) == ESRCH) {
      LOG(ERROR) << "pid=" << edgeIter.second->pid_ << " is dead";
      // TODO detach the EdgeQueue
    }
  }
  LOG(INFO) << "watchdog found registered edge processes=" << edgeRegistry_.size();
  return 0;
}
//
// ============== RORAGATEWAY ===========================

int RoraGateway::init(const std::string& configFileName, int argc, const char* argv[]) {
  int ret = 0;

  ret = config_.readConfig(configFileName, argc, argv);
  if (ret != 0) {
    LOG(ERROR) << "failed to read config=" << configFileName;
    return -1;
  }

  edges_.epoller_.init();

  watchDogPtr_ = gobjfs::make_unique<TimerNotifier>(config_.watchDogTimeSec_, 0);

  ret = edges_.epoller_.addEvent(0, watchDogPtr_->getFD(), 
      EPOLLIN,
      std::bind(&RoraGateway::watchDogFunc, this, 
        std::placeholders::_1,
        std::placeholders::_2));
  if (ret != 0) {
    LOG(ERROR) << "failed to add watchdog timer ret=" << ret;
    return -1;
  }

  adminQueuePtr_ = gobjfs::make_unique<AdminQueue>(version_, config_.maxAdminQueueLen_);

  return ret;
}

int RoraGateway::addASD(const std::string& transport,
    const std::string& ipAddress,
    const int port,
    int edgePid) {

  int ret = 0;

  decltype(asdVec_)::iterator iter;
  decltype(asdVec_)::iterator end = asdVec_.end();

  iter = std::find_if(asdVec_.begin(), asdVec_.end(),
      [&] (const ASDInfoUPtr& p) -> bool { 
        return (ipAddress == p->ipAddress_
          && port == p->port_ 
          && transport == p->transport_);
      });

  if (iter != asdVec_.end()) {
    auto& asdPtr = *iter;
    assert(asdPtr->edgesUsingMe_.size() > 0);
    asdPtr->edgesUsingMe_.insert(edgePid);
    LOG(INFO) << "Edge=" << edgePid << " is registered user of ASD=" 
      << ipAddress << ":" << port;
  } else {
    try {
      auto asdp = gobjfs::make_unique<ASDInfo>(this,
        transport, ipAddress, port, 
        GatewayMsg::MaxMsgSize, 
        config_.maxASDQueueLen_,
        config_.maxThreadsPerASD_,
        config_.maxConnPerASD_); 

      asdp->edgesUsingMe_.insert(edgePid);
      LOG(INFO) << "Edge=" << edgePid << " has added and registered ASD=" 
        << ipAddress << ":" << port;
  
      // start threads
      for (size_t thrIdx = 0; thrIdx < asdp->maxThreadsPerASD_; thrIdx ++) {
        asdp->asdThreadVec_.push_back(std::make_shared<ASDThreadInfo>());
        asdp->asdThreadVec_[thrIdx]->thread_ = std::thread(
            std::bind(&ASDThreadInfo::threadFunc, asdp->asdThreadVec_[thrIdx].get(), this, asdp.get(), thrIdx));
      }
  
      asdVec_.push_back(std::move(asdp));

    } catch (const std::exception& e) {
      LOG(ERROR) << "failed to add connection to ASD=" << ipAddress << ":" << port;
      ret = -1;
    }
  }

  return ret;
}

int RoraGateway::dropASD(const std::string& transport,
    const std::string& ipAddress,
    const int port,
    int edgePid) {
  
  int ret = -1;
  decltype(asdVec_)::iterator iter;
  decltype(asdVec_)::iterator end = asdVec_.end();

  ASDInfo* asdPtr = nullptr;
  for (iter = asdVec_.begin(); iter != end; ++ iter) {
    asdPtr = iter->get();
    if ((asdPtr->transport_ == transport) && 
        (asdPtr->ipAddress_ == ipAddress) && 
        (asdPtr->port_ == port)) {
      break;
    }
  }

  if (asdPtr) {
    if (asdPtr->edgesUsingMe_.find(edgePid) != asdPtr->edgesUsingMe_.end()) {
      if (asdPtr->edgesUsingMe_.size() > 1) {
        LOG(INFO) << "removed edge=" << edgePid << " from users of ASD=" << ipAddress << ":" << port;
        asdPtr->edgesUsingMe_.erase(edgePid);
      } else {

        LOG(INFO) << "deleting ASD=" << ipAddress << ":" << port << " by edge=" << edgePid;
        asdPtr->destroyAll(this);
  
        // remove the entry from list of registered ASDs
        asdVec_.erase(iter);

        ret = 0;
  
        LOG(INFO) << "deleted ASD=" << ipAddress << ":" << port;
      }
    } else {
      LOG(ERROR) << "Invalid drop request. "
        "edge=" << edgePid << " not registered user of ASD=" << ipAddress << ":" << port;
    }
  } else {
    LOG(ERROR) << "Not found ASD for " << ipAddress << ":" << port;
  }
  return ret;
}

/**
 * TODO : enable timer when 1st edge registers, disable when none
 */
int RoraGateway::watchDogFunc(int fd, uintptr_t userCtx) {
  uint64_t numPendingEvents = 0;
  int ret = TimerNotifier::recv(fd, numPendingEvents);
  if (ret == 0) {
    edges_.cleanupForDeadEdgeProcesses();
  } else {
    LOG(ERROR) << "failed to read timer ret=" << ret;
  }

  {
    // Print stats for writes to each EdgeQueue 
    std::unique_lock<std::mutex> l(edges_.edgeRegistryMutex_);
    if (edges_.edgeRegistry_.size() == 0) {
      // return early if no registered processes
      return 0;
    }
    for (auto& edgeIter : edges_.edgeRegistry_) {
      auto& edgeQueue = edgeIter.second;
      auto str = edgeQueue->getStats();
      edgeQueue->clearStats();
      LOG(INFO) << "EdgeQueue=" << edgeQueue->pid_ << ",stats=" << str;
    }
  }

  // Print stats for reads from each ASD queue 
  // TODO may need mutex once dynamic registration of ASD is allowed
  for (auto& asdInfo : asdVec_) {
    auto str = asdInfo->queue_->getStats();
    LOG(INFO) << "ASD=" << (void*)asdInfo.get() << ":" << asdInfo->ipAddress_ << ":" << asdInfo->port_  
      << ",queueStats=" << str
      << ",submitBatchSize=" << asdInfo->stats_.submitBatchSize_
      << ",callbackBatchSize=" << asdInfo->stats_.callbackBatchSize_;
    asdInfo->clearStats();
  }
  
  return 0;
}

int RoraGateway::adminThreadFunc() {

  adminThread_.threadId_ = gettid();
  adminThread_.started_ = true;
  LOG(INFO) << "started admin thread=" << gettid();

  int timeout_ms = 1;
  while (not adminThread_.stopping_) {
    GatewayMsg adminMsg;
    int ret = adminQueuePtr_->timed_read(adminMsg, timeout_ms);
    if (ret != 0) {
      if (timeout_ms < 1000) {
        timeout_ms = timeout_ms << 1;
      }
      continue;
    } else {
      // got a request, reset timeout interval
      timeout_ms = 1;
    }

    assert(ret == 0);

    switch(adminMsg.opcode_) 
    {
      case Opcode::ADD_ASD_REQ:
        {
          LOG(INFO) << "got add asd for " << adminMsg.ipAddress_ << ":" << adminMsg.port_ << " from " << adminMsg.edgePid_;

          auto edgePtr = edges_.find(adminMsg.edgePid_);
          if (edgePtr) {

            ret = addASD(adminMsg.transport_, adminMsg.ipAddress_, adminMsg.port_, 
              adminMsg.edgePid_);

          } else {
            LOG(ERROR) << " could not find edge pid=" << adminMsg.edgePid_;
            ret = -1;
          }

          // TODO set retval
          edgePtr->writeResponse(createAddASDResponse(ret));

          break;
        }
      case Opcode::DROP_ASD_REQ:
        {
          LOG(INFO) << "got drop asd for " << adminMsg.ipAddress_ << ":" << adminMsg.port_;

          auto edgePtr = edges_.find(adminMsg.edgePid_);
          if (edgePtr) {

            ret = dropASD(adminMsg.transport_,
              adminMsg.ipAddress_,
              adminMsg.port_,
              adminMsg.edgePid_);

          } else {
            LOG(ERROR) << " could not find queue for pid=" << adminMsg.edgePid_;
            ret = -1;
          }

          edgePtr->writeResponse(createDropASDResponse(ret));

          break;
        }
      case Opcode::ADD_EDGE_REQ:
        {
          LOG(INFO) << "got open from edge process=" << adminMsg.edgePid_;
          auto edgePtr = std::make_shared<EdgeQueue>(adminMsg.edgePid_);
          assert(edgePtr->pid_ == adminMsg.edgePid_);
          edges_.insert(edgePtr);

          edgePtr->writeResponse(createAddEdgeResponse(adminMsg.edgePid_, ret));
          break;
        }
      case Opcode::DROP_EDGE_REQ:
        {
          LOG(INFO) << "got close from edge process=" << adminMsg.edgePid_;
          // TODO add ref counting to ensure edge queue doesnt go away
          // while there are outstanding requests
          auto edgePtr = edges_.find(adminMsg.edgePid_);
          int ret = -1;

          if (edgePtr) {
            ret = 0;
            edgePtr->writeResponse(createDropEdgeResponse(adminMsg.edgePid_, ret));
            // cannot drop edge until response sent back
            ret = edges_.drop(adminMsg.edgePid_);
          } else {
            ret = -1;
            edgePtr->writeResponse(createDropEdgeResponse(adminMsg.edgePid_, ret));
            LOG(ERROR) << " could not find queue for pid=" << adminMsg.edgePid_;
          }
          break;
        }
      default:
        {
          auto edgePtr = edges_.find(adminMsg.edgePid_);
          if (edgePtr) {
            edgePtr->writeResponse(createInvalidResponse(adminMsg.edgePid_, -1));
          } else {
            LOG(ERROR) << " cannot find edge queue for pid=" << adminMsg.edgePid_;
          }
          LOG(ERROR) << " admin got unknown opcode=" << adminMsg.opcode_ << " from pid=" << adminMsg.edgePid_;
          break;
        }
      }
  }

  adminThread_.stopped_ = true;
  LOG(INFO) << "stopped admin thread=" << gettid();
  return 0;
}

int RoraGateway::ASDThreadInfo::threadFunc(RoraGateway* rgPtr, ASDInfo* asdInfo, size_t thrIdx) {

  RoraGateway::ASDThreadInfo* threadInfo = asdInfo->asdThreadVec_[thrIdx].get();
  assert(threadInfo == this);
  threadId_ = gettid();
  started_ = true;

  LOG(INFO) << "started asd thread=" << gettid() 
    << ",using thread slot=" << thrIdx 
    << ",uri=" << asdInfo->ipAddress_ << ":" << asdInfo->port_;

  nextConnIdx_ = thrIdx;

  while (1) {

    int ret = 0;
    GatewayMsg anyReq;
    if (stopping_ || pending_giocb_vec_.size()) {
      // see if there are more read requests we can batch
      ret = asdInfo->queue_->try_read(anyReq);
      if (ret != 0) {
        numIdleLoops_ ++;
      } else {
        numIdleLoops_ = 0;
      }
    } else {
      // if nothing in pending_giocb_vec_ queue, do longer wait.
      // Cannot do blocking wait here otherwise process termination would 
      // require having to write a termination message to the queue 
      // from inside this process (from the thread calling shutdown)
      ret = asdInfo->queue_->timed_read(anyReq, timeout_ms_);
      numIdleLoops_ = 0;
      if (ret != 0) {
        // if no writers, keep doubling timeout to save CPU
        // but bound the timeout to max one second so process terminates
        // quickly on being notified of shutdown
        if (timeout_ms_ < 1000) {
          timeout_ms_ = timeout_ms_ << 1;
        }
        // assert cannot be sleeping when there are pending requests
        assert(pending_giocb_vec_.empty());
        continue;
      } else {
        // got a request, reset timeout interval
        timeout_ms_ = 1;
      }
    } 

    if (ret == 0) { 
      switch (anyReq.opcode_) {

      case Opcode::READ_REQ:
        {
          const int pid = (pid_t)anyReq.edgePid_;
          auto edgePtr = rgPtr->edges_.find(pid);
          
          if (edgePtr) {
            LOG(DEBUG) << "got read from pid=" << pid << " for number=" << anyReq.numElems();
            auto giocb_vec = edgePtr->giocb_from_GatewayMsg(anyReq);
            // put iocb into pending_giocb_vec_ queue
            if (pending_giocb_vec_.empty()) {
              pending_giocb_vec_ = std::move(giocb_vec);
            } else {
              pending_giocb_vec_.reserve(
                pending_giocb_vec_.size() + giocb_vec.size());
              std::move(std::begin(giocb_vec),
                std::end(giocb_vec),
                std::back_inserter(pending_giocb_vec_));
              giocb_vec.clear();
            }
          } else {
            LOG(ERROR) << " could not find queue for pid=" << pid;
          }
          break;
        }
      default:
        {
          LOG(ERROR) << " got unknown opcode=" << anyReq.opcode_;
          break;
        }
      }
    }
      // submit whatever is pending_giocb_vec_ if
      // (1) have as many requests as number of edges
      // (2) got nothing on previous nonblocking read AND have some pending
      //     requests 
      if ((pending_giocb_vec_.size() >= rgPtr->edges_.size()) || 
          ((numIdleLoops_ == 1) && pending_giocb_vec_.size())) {

          asdInfo->stats_.submitBatchSize_ = pending_giocb_vec_.size();

          client_ctx_ptr& ctx = asdInfo->ctxVec_[nextConnIdx_];

          auto aio_ret = aio_readv(ctx, pending_giocb_vec_);
          if (aio_ret != 0) {
            // got an error? send back response right now
            // TODO : this doesnt handle case where some of the reads
            // were submitted successfully
            for (auto iocb : pending_giocb_vec_) {
              auto edgePtr = rgPtr->edges_.find(iocb->user_ctx);
  
              if (edgePtr) {
                GatewayMsg respMsg;
                edgePtr->GatewayMsg_from_giocb(respMsg, *iocb, -EIO);
                auto ret = edgePtr->writeResponse(respMsg);
                assert(ret == 0);
              } else {
                LOG(ERROR) << "could not find edgeQueue for pid=" << iocb->user_ctx;
              }
              delete iocb;
            }
          } else  {
            aio_wait_all(ctx);
          }
          pending_giocb_vec_.clear();
          // number of threads is less than or equal to number of connections
          // rotate the connection used by this thread for sending msgs
          // for example
          // maxConn | maxThr | thread should use these connections
          //   8        1         (1, 2, 3, 4, ..)
          //   8        2         (1, 3, 5, 7)
          //   8        4         (1, 5)
          //   8        8         (1)
          nextConnIdx_ = (nextConnIdx_ + asdInfo->maxThreadsPerASD_) % asdInfo->ctxVec_.size();
      }
      if (pending_giocb_vec_.empty() && stopping_) {
        break;
      }
  }

  assert(pending_giocb_vec_.empty());
  stopped_ = true;
  LOG(INFO) << "stopped asd thread=" << threadId_
    << ",using thread slot=" << thrIdx 
    << ",uri=" << asdInfo->ipAddress_ << ":" << asdInfo->port_;
  return 0;
}

int RoraGateway::handleReadCompletion(int fd, uintptr_t ptr) {

  uint64_t doneCount =0; // move to ASDInfo

  ASDInfo::CallbackInfo* callbackCtx = reinterpret_cast<ASDInfo::CallbackInfo*>(ptr);
  ASDInfo* asdPtr = callbackCtx->asdPtr_;
  client_ctx_ptr& ctx = asdPtr->ctxVec_[callbackCtx->connIdx_];

  std::vector<giocb*> iocb_vec;
  int times = 1; // ignored right now
  int r = aio_getevents(ctx, times, iocb_vec);

  if (r == 0) {
    asdPtr->stats_.callbackBatchSize_ = iocb_vec.size();
    for (auto& iocb : iocb_vec) {

      const int pid = (int) iocb->user_ctx;

      auto edgePtr = edges_.find(pid);
      if (edgePtr) {

        LOG(DEBUG) << "send response to pid=" << pid 
          << ",ret=" << aio_return(iocb)
          << ",filename=" << iocb->filename;
        GatewayMsg respMsg;
        edgePtr->GatewayMsg_from_giocb(respMsg, *iocb, 
            aio_return(iocb));
        auto ret = edgePtr->writeResponse(respMsg);
        assert(ret == 0);
      } else {
        LOG(ERROR) << "not found edge queue for pid=" << pid;
      }

      aio_finish(iocb);
      delete iocb;
    }
    doneCount += iocb_vec.size();
  }
  return 0;
}

/**
 * 1) fetch responses from all xio ctx and write them to edgeQueue
 * 2) handle watchdog timer
 */
int RoraGateway::responseThreadFunc(size_t thrIdx) {

  edges_.epollerThreadsVec_[thrIdx]->started_ = true;
  LOG(INFO) << "started edge response thread=" << gettid() << " idx=" << thrIdx;

  while (!edges_.epollerThreadsVec_[thrIdx]->stopping_) {
    edges_.epoller_.run(1);
  }

  LOG(INFO) << "stopped edge response thread=" << gettid() << " idx=" << thrIdx;
  edges_.epollerThreadsVec_[thrIdx]->stopped_ = true;
  return 0;
}

int RoraGateway::run() {
  int ret = 0;

  adminThread_.thread_ = std::thread(
      std::bind(&RoraGateway::adminThreadFunc, this));

  for (size_t idx = 0; idx < config_.maxEPollerThreads_; idx ++) {
    edges_.epollerThreadsVec_.push_back(std::make_shared<ThreadInfo>()); 
    edges_.epollerThreadsVec_[idx]->thread_ = std::thread(std::bind(&RoraGateway::responseThreadFunc, this, idx));
  }

  // hang the thread until someone calls shutdown
  for (auto& thrInfo : edges_.epollerThreadsVec_) {
    thrInfo->thread_.join();
  }

  for (auto& asdPtr : asdVec_) {
    for (auto& threadInfo : asdPtr->asdThreadVec_) {
      threadInfo->thread_.join();
    }
  }
  asdVec_.clear();

  adminThread_.thread_.join();

  return ret;
}

int RoraGateway::shutdown() {
  int ret = 0;

  for (auto& thrInfo : edges_.epollerThreadsVec_) {
    thrInfo->stopping_ = true;
  }

  edges_.epoller_.shutdown();

  for (auto& asdPtr : asdVec_) {
    for (auto& threadInfo : asdPtr->asdThreadVec_) {
      threadInfo->stopping_ = true;
    }
  }

  adminThread_.stopping_ = true;

  return ret;
}

}
}
