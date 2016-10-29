
#include "RoraGateway.h"
#include <rora/GatewayProtocol.h>

#include <boost/program_options.hpp>
#include <gobjfs_log.h>
#include <util/lang_utils.h>
#include <util/os_utils.h>

namespace bpo = boost::program_options;
using namespace gobjfs::xio;
using gobjfs::os::EPoller;

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

RoraGateway::ASDInfo::ASDInfo(const std::string &transport, 
    const std::string &ipAddress, 
    int port,
    size_t maxMsgSize,
    size_t maxQueueLen,
    size_t maxThreadsPerASD) {

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
    int err = ctx_init(ctx);
    assert(err == 0);

    auto threadInfo = std::make_shared<ThreadInfo>();
    threadInfo->ctx_ = ctx;

    threadVec_.push_back(threadInfo);
  }
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



int RoraGateway::init(const std::string& configFileName) {
  int ret = 0;

  Config config;
  ret = config.readConfig(configFileName);
  if (ret != 0) {
    LOG(ERROR) << "failed to read config=" << configFileName;
    return -1;
  }

  edges_.epoller_.init();

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

      edges_.epoller_.addEvent(reinterpret_cast<uintptr_t>(callbackCtx.get()), 
        aio_geteventfd(asdp->threadVec_[idx]->ctx_), 
        EPOLLIN, 
        std::bind(&RoraGateway::handleReadCompletion, this,
          std::placeholders::_1,
          std::placeholders::_2));

      asdp->callbackInfoList_.push_back(callbackCtx);
    }

    // access the asdp before std::move
    asdVec_.push_back(std::move(asdp));
  }

  return ret;
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

  int idleLoop = 0;

  while (1) 
  {
    int ret = 0;
    GatewayMsg anyReq;
    if (threadInfo->stopping_ || pending_giocb_vec.size()) {
      // see if there are more read requests we can batch
      ret = asdInfo->queue_->try_read(anyReq);
      if (ret != 0) {
        idleLoop ++;
      } else {
        idleLoop = 0;
      }
    } else {
      // if nothing in pending_giocb_vec queue, do longer wait.
      // NB -- cant do blocking wait here otherwise process termination would 
      // require having to write a termination message to the queue 
      // from inside this process (from the thread calling shutdown)
      ret = asdInfo->queue_->timed_read(anyReq, timeout_ms);
      if (ret != 0) {
        // if no writers, keep doubling timeout to save CPU
        // bound the timeout to max one second so process terminates
        // quickly on being notified
        if (timeout_ms < 1000) {
          timeout_ms = timeout_ms << 1;
        }
        idleLoop = 0;
        // assert cant be sleeping when there are pending requests
        assert(pending_giocb_vec.empty());
        continue;
      } else {
        // got a request, reset timeout interval
        timeout_ms = 1;
        idleLoop = 0;
      }
    } 

    switch (anyReq.opcode_) {

      case Opcode::OPEN:
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
      case Opcode::READ:
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
      case Opcode::CLOSE:
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
        break;
    }
      // submit whatever is pending_giocb_vec if
      // (1) have as many requests as number of edges
      // (2) idle && got some requests 
      if ((pending_giocb_vec.size() > edges_.size()) || 
          (idleLoop && pending_giocb_vec.size())) {

          asdInfo->stats_.submitBatchSize_ = pending_giocb_vec.size();
          auto aio_ret = aio_readv(ctx, pending_giocb_vec);
          if (aio_ret != 0) {
            // got an error, send back error
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
    << ",uri=" << asdInfo->ipAddress_ << ":" << asdInfo->port_
    << ",submitBatchSize=" << asdInfo->stats_.submitBatchSize_
    << ",callbackBatchSize=" << asdInfo->stats_.callbackBatchSize_;
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
        respMsg.opcode_ = Opcode::READ_RESP;
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
 * fetch responses from all xio ctx and
 * write them to edgeQueue
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
