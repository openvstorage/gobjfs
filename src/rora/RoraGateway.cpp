
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

struct Config {

  std::vector<std::string> transportVec_;
  std::vector<std::string> ipAddressVec_;
  std::vector<int> portVec_;

  size_t maxQueueLen_ {256};
  // client threads to connect to multiple portals
  size_t maxThreadsPerASD_{1}; 

  int readConfig(const std::string& configFileName) {

    bpo::options_description desc("allowed opt");
    desc.add_options()
        ("ipaddress", bpo::value<std::vector<std::string>>(&ipAddressVec_)->required(), "ASD ip address")
        ("port", bpo::value<std::vector<int>>(&portVec_)->required(), "ASD port")
        ("transport", bpo::value<std::vector<std::string>>(&transportVec_)->required(), "ASD transport : tcp or rdma")
        ("max_asd_queue", bpo::value<size_t>(&maxQueueLen_)->required(), "length of shared memory queue used to forward requests to ASD")
        ("max_threads_per_asd", bpo::value<size_t>(&maxThreadsPerASD_)->required(), "max threads connecting to an ASD (exploit accelio portals)")
        ;

    std::ifstream configFile(configFileName);
    bpo::variables_map vm;
    bpo::store(bpo::parse_config_file(configFile, desc), vm);
    bpo::notify(vm);

    if ((transportVec_.size() != ipAddressVec_.size()) ||
      transportVec_.size() != portVec_.size()) {
      LOG(ERROR) << "ASD mismatch transport entries=" << transportVec_.size()
        << " ipaddress entries=" << ipAddressVec_.size()
        << " port entries=" << portVec_.size();
      return -1;
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
};

const int RoraGateway::watchDogTimeSec_ = 30;  // TODO tunable

RoraGateway::ASDInfo::ASDInfo(const std::string &transport, 
    const std::string &ipAddress, 
    int port,
    size_t maxMsgSize,
    size_t maxQueueLen,
    size_t maxThreadsPerASD) 
  : transport_(transport)
  , ipAddress_(ipAddress)
  , port_(port) {

  const std::string asdQueueName = ipAddress + ":" + std::to_string(port);

  queue_ = gobjfs::make_unique<ASDQueue>(asdQueueName, maxQueueLen, maxMsgSize);

  auto ctx_attr_ptr = ctx_attr_new();
  ctx_attr_set_transport(ctx_attr_ptr, transport, ipAddress, port);

  threadVec_.reserve(maxThreadsPerASD);
  // Many threads can read from one shmem queue and 
  // forward requests to the server independently
  for (size_t idx = 0; idx < maxThreadsPerASD; idx ++) {
    auto ctx = ctx_new(ctx_attr_ptr);
    assert(ctx);
    int err = ctx_init(ctx, maxQueueLen);
    assert(err == 0);

    auto threadInfo = std::make_shared<ThreadInfo>();
    threadInfo->ctx_ = ctx;

    threadVec_.push_back(threadInfo);
  }
}

void RoraGateway::ASDInfo::clearStats() {
  stats_.submitBatchSize_.reset();
  stats_.callbackBatchSize_.reset();
  queue_->clearStats();
}

EdgeQueueSPtr RoraGateway::EdgeInfo::find(int pid) {
  std::unique_lock<std::mutex> l(mutex_);
  auto iter = catalog_.find(pid);
  if (iter != catalog_.end()) {
    return iter->second;
  }
  return nullptr;
}

int RoraGateway::EdgeInfo::drop(int pid) {
  std::unique_lock<std::mutex> l(mutex_);
  size_t sz = catalog_.erase(pid);
  if (sz != 1) {
    LOG(ERROR) << "Failed to find edge entry for pid=" << pid;
    return -1;
  }
  return 0;
}

size_t RoraGateway::EdgeInfo::size() const {
  std::unique_lock<std::mutex> l(mutex_);
  return catalog_.size();
}

int RoraGateway::EdgeInfo::insert(EdgeQueueSPtr edgePtr) {
  std::unique_lock<std::mutex> l(mutex_);
  catalog_.insert(std::make_pair(edgePtr->pid_, edgePtr));
  return 0;
}

int RoraGateway::EdgeInfo::cleanupForDeadEdgeProcesses() {
  std::unique_lock<std::mutex> l(mutex_, std::try_to_lock);

  if (!l.owns_lock()) {
    LOG(WARNING) << "failed to obtain edge catalog mutex.  returning";
    return 0;
  }
  if (!catalog_.size()) {
    return 0;
  }
  for (auto& edgeIter : catalog_) {
    if (kill(edgeIter.second->pid_, 0) == ESRCH) {
      LOG(ERROR) << "pid=" << edgeIter.second->pid_ << " is dead";
      // TODO detach the EdgeQueue
    }
  }
  LOG(INFO) << "watchdog found registered edge processes=" << catalog_.size();
  return 0;
}

int RoraGateway::init(const std::string& configFileName) {
  int ret = 0;

  Config config;
  ret = config.readConfig(configFileName);
  if (ret != 0) {
    LOG(ERROR) << "failed to read config=" << configFileName;
    return -1;
  }

  edges_.epoller_.init();

  watchDogPtr_ = gobjfs::make_unique<TimerNotifier>(watchDogTimeSec_, 0);

  ret = edges_.epoller_.addEvent(0, watchDogPtr_->getFD(), 
      EPOLLIN,
      std::bind(&RoraGateway::watchDogFunc, this, 
        std::placeholders::_1,
        std::placeholders::_2));
  if (ret != 0) {
    LOG(ERROR) << "failed to add watchdog timer ret=" << ret;
    return -1;
  }

  size_t numASD = config.transportVec_.size();
  asdVec_.reserve(numASD);
  for (size_t idx = 0; idx < numASD; idx ++) {

    auto& transport = config.transportVec_[idx];
    auto& port = config.portVec_[idx];
    auto& ipAddress = config.ipAddressVec_[idx];

    auto asdp = gobjfs::make_unique<ASDInfo>(transport, ipAddress, port, 
        GatewayMsg::MaxMsgSize, config.maxQueueLen_,
        config.maxThreadsPerASD_); 

    // for each ASD connection, add an event to monitor for read completions
    // TODO need to call dropEvent ?
    for (size_t idx = 0; idx < asdp->threadVec_.size(); idx ++) {

      auto callbackCtx = std::make_shared<ASDInfo::CallbackInfo>();
      callbackCtx->asdPtr_ = asdp.get();
      callbackCtx->connIdx_ = idx;
      int fd_to_add = aio_geteventfd(asdp->threadVec_[idx]->ctx_), 

      ret = edges_.epoller_.addEvent(reinterpret_cast<uintptr_t>(callbackCtx.get()), 
        fd_to_add,
        EPOLLIN, 
        std::bind(&RoraGateway::handleReadCompletion, this,
          std::placeholders::_1,
          std::placeholders::_2));
      if (ret != 0) {
        LOG(ERROR) << "failed to add fd=" << fd_to_add 
          << " for asd uri=" << ipAddress << ":" <<port 
          << " conn_index=" << idx;
        break;
      } else {
        // save callback info in a shared_ptr list so it doesnt get destroyed
        asdp->callbackInfoList_.push_back(callbackCtx);
      }
    }

    if (ret == 0) {
      // access the asdp before std::move
      asdVec_.push_back(std::move(asdp));
    } else {
      break;
    }
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
    std::unique_lock<std::mutex> l(edges_.mutex_);
    if (edges_.catalog_.size() == 0) {
      // return early if no registered processes
      return 0;
    }
    for (auto& edgeIter : edges_.catalog_) {
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

int RoraGateway::asdThreadFunc(ASDInfo* asdInfo, size_t connIdx) {

  ThreadInfo* threadInfo = asdInfo->threadVec_[connIdx].get();
  threadInfo->started_ = true;

  LOG(INFO) << "started asd thread=" << gettid() 
    << ",using connection number=" << connIdx 
    << ",uri=" << asdInfo->ipAddress_ << ":" << asdInfo->port_;

  client_ctx_ptr ctx = threadInfo->ctx_;
  int timeout_ms = 1;

  std::vector<giocb*> pending_giocb_vec;

  int numIdleLoops = 0;

  while (1) 
  {
    int ret = 0;
    GatewayMsg anyReq;
    if (threadInfo->stopping_ || pending_giocb_vec.size()) {
      // see if there are more read requests we can batch
      ret = asdInfo->queue_->try_read(anyReq);
      if (ret != 0) {
        numIdleLoops ++;
      } else {
        numIdleLoops = 0;
      }
    } else {
      // if nothing in pending_giocb_vec queue, do longer wait.
      // Cannot do blocking wait here otherwise process termination would 
      // require having to write a termination message to the queue 
      // from inside this process (from the thread calling shutdown)
      ret = asdInfo->queue_->timed_read(anyReq, timeout_ms);
      numIdleLoops = 0;
      if (ret != 0) {
        // if no writers, keep doubling timeout to save CPU
        // but bound the timeout to max one second so process terminates
        // quickly on being notified of shutdown
        if (timeout_ms < 1000) {
          timeout_ms = timeout_ms << 1;
        }
        // assert cannot be sleeping when there are pending requests
        assert(pending_giocb_vec.empty());
        continue;
      } else {
        // got a request, reset timeout interval
        timeout_ms = 1;
      }
    } 

    if (ret == 0) { 
      switch (anyReq.opcode_) {

      case Opcode::OPEN_REQ:
        {
          LOG(INFO) << "got open from edge process=" << anyReq.edgePid_;
          auto edgePtr = std::make_shared<EdgeQueue>(anyReq.edgePid_);
          assert(edgePtr->pid_ == anyReq.edgePid_);
          edges_.insert(edgePtr);

          GatewayMsg respMsg;
          respMsg.opcode_ = Opcode::OPEN_RESP;
          edgePtr->write(respMsg);
          break;
        }
      case Opcode::READ_REQ:
        {
          const int pid = (pid_t)anyReq.edgePid_;
          auto edgePtr = edges_.find(pid);
          
          if (edgePtr) {
            LOG(DEBUG) << "got read from pid=" << pid << " for file=" << anyReq.filename_;
            giocb* iocb = edgePtr->giocb_from_GatewayMsg(anyReq);
            // put iocb into pending_giocb_vec queue
            pending_giocb_vec.push_back(iocb);
          } else {
            LOG(ERROR) << " could not find queue for pid=" << pid;
          }
          break;
        }
      case Opcode::CLOSE_REQ:
        {
          LOG(INFO) << "got close from edge process=" << anyReq.edgePid_;
          // TODO add ref counting to ensure edge queue doesnt go away
          // while there are outstanding requests
          auto edgePtr = edges_.find(anyReq.edgePid_);

          if (edgePtr) {
            GatewayMsg respMsg;
            respMsg.opcode_ = Opcode::CLOSE_RESP;
            edgePtr->write(respMsg);
            int ret = edges_.drop(anyReq.edgePid_);
            (void) ret;
          } else {
            LOG(ERROR) << " could not find queue for pid=" << anyReq.edgePid_;
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
      // submit whatever is pending_giocb_vec if
      // (1) have as many requests as number of edges
      // (2) got nothing on previous nonblocking read AND have some pending
      //     requests 
      if ((pending_giocb_vec.size() > edges_.size()) || 
          ((numIdleLoops == 1) && pending_giocb_vec.size())) {

          asdInfo->stats_.submitBatchSize_ = pending_giocb_vec.size();
          auto aio_ret = aio_readv(ctx, pending_giocb_vec);
          if (aio_ret != 0) {
            // got an error? send back response right now
            // TODO : this doesnt handle case where some of the reads
            // were submitted successfully
            for (auto iocb : pending_giocb_vec) {
              auto edgePtr = edges_.find(iocb->user_ctx);
  
              if (edgePtr) {
                GatewayMsg respMsg;
                edgePtr->GatewayMsg_from_giocb(respMsg, *iocb, 
                  -1, EIO);
                auto ret = edgePtr->write(respMsg);
                assert(ret == 0);
              } else {
                LOG(ERROR) << "could not find edgeQueue for pid=" << iocb->user_ctx;
              }
              delete iocb;
            }
          } else  {
            aio_wait_all(ctx);
          }
          pending_giocb_vec.clear();
      }
      if (pending_giocb_vec.empty() && threadInfo->stopping_) {
        break;
      }
  }

  assert(pending_giocb_vec.empty());
  threadInfo->stopped_ = true;
  LOG(INFO) << "stopped asd thread=" << gettid() 
    << ",using connection number=" << connIdx 
    << ",uri=" << asdInfo->ipAddress_ << ":" << asdInfo->port_;
  return 0;
}

int RoraGateway::handleReadCompletion(int fd, uintptr_t ptr) {

  uint64_t doneCount =0; // move to ASDInfo

  ASDInfo::CallbackInfo* callbackCtx = reinterpret_cast<ASDInfo::CallbackInfo*>(ptr);
  ASDInfo* asdPtr = callbackCtx->asdPtr_;
  client_ctx_ptr ctx = asdPtr->threadVec_[callbackCtx->connIdx_]->ctx_;

  std::vector<giocb*> iocb_vec;
  int times = 1; // ignored right now
  int r = aio_getevents(ctx, times, iocb_vec);

  if (r == 0) {
    asdPtr->stats_.callbackBatchSize_ = iocb_vec.size();
    for (auto& iocb : iocb_vec) {

      const int pid = (int) iocb->user_ctx;

      auto edgePtr = edges_.find(pid);
      if (edgePtr) {

        GatewayMsg respMsg;
        edgePtr->GatewayMsg_from_giocb(respMsg, *iocb, 
            aio_return(iocb), aio_error(iocb));
        LOG(DEBUG) << "send response to pid=" << pid 
          << ",ret=" << aio_return(iocb)
          << ",err=" << aio_error(iocb)
          << ",filename=" << iocb->filename;
        auto ret = edgePtr->write(respMsg);
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
 * 1) fetch responses from all xio ctx and
 * write them to edgeQueue
 * 2) handle watchdog timer
 */
int RoraGateway::responseThreadFunc() {

  LOG(INFO) << "started edge response thread=" << gettid();

  while (!edges_.stopping_) {
    edges_.epoller_.run(1);
  }

  LOG(INFO) << "stopped edge response thread=" << gettid();
  return 0;
}

int RoraGateway::run() {
  int ret = 0;

  for (auto& asdPtr : asdVec_) {
    for (size_t connIdx = 0; connIdx < asdPtr->threadVec_.size(); connIdx ++) {
      asdPtr->threadVec_[connIdx]->thread_ = std::thread(
          std::bind(&RoraGateway::asdThreadFunc, this, asdPtr.get(), connIdx));
    }
  }

  edges_.thread_ = std::thread(std::bind(&RoraGateway::responseThreadFunc, this));

  edges_.thread_.join();

  for (auto& asdPtr : asdVec_) {
    for (auto& threadInfo : asdPtr->threadVec_) {
      threadInfo->thread_.join();
    }
  }
  asdVec_.clear();

  return ret;
}

int RoraGateway::shutdown() {
  int ret = 0;

  edges_.stopping_ = true;
  edges_.epoller_.shutdown();

  for (auto& asdPtr : asdVec_) {
    for (auto& threadInfo : asdPtr->threadVec_) {
      threadInfo->stopping_ = true;
    }
  }

  return ret;
}

}
}
