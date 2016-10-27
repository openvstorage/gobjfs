
#include "RoraGateway.h"
#include <rora/GatewayProtocol.h>

#include <boost/program_options.hpp>
#include <glog/logging.h>
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

  // TODO put in config
  size_t maxQueueLen_ {256};

  int readConfig(const std::string& configFileName) {

    bpo::options_description desc("allowed opt");
    desc.add_options()
        ("ipaddress", bpo::value<std::vector<std::string>>(&ipAddressVec_)->required(), "ASD ip address")
        ("port", bpo::value<std::vector<int>>(&portVec_)->required(), "ASD port")
        ("transport", bpo::value<std::vector<std::string>>(&transportVec_)->required(), "ASD transport : tcp or rdma")
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
    size_t maxQueueLen) {

  const std::string asdQueueName = ipAddress + ":" + std::to_string(port);

  queue_ = gobjfs::make_unique<ASDQueue>(asdQueueName, maxMsgSize, maxQueueLen);

  auto ctx_attr_ptr = ctx_attr_new();
  ctx_attr_set_transport(ctx_attr_ptr, transport, ipAddress, port);

  ctx_ = ctx_new(ctx_attr_ptr);
  assert(ctx_);

  int err = ctx_init(ctx_);
  assert(err == 0);
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

  for (size_t idx = 0; idx < numASD; idx ++) {

    auto& transport = config.transportVec_[idx];
    auto& port = config.portVec_[idx];
    auto& ipAddress = config.ipAddressVec_[idx];

    auto asdp = gobjfs::make_unique<ASDInfo>(transport, ipAddress, port, GatewayMsg::MaxMsgSize, config.maxQueueLen_); 

    // for each ASD, add an event to monitor for read completions
    edges_.epoller_.addEvent(reinterpret_cast<uintptr_t>(asdp.get()), 
        aio_geteventfd(asdp->ctx_), 
        EPOLLIN, 
        std::bind(&RoraGateway::handleReadCompletion, this,
          std::placeholders::_1,
          std::placeholders::_2));

    // use the asdp before std::move
    asdList_.push_back(std::move(asdp));
  }


  return ret;
}

int RoraGateway::asdThreadFunc(ASDInfo* asdInfo) {

  asdInfo->started_ = true;

  LOG(INFO) << "started asd thread=" << gettid() << " for uri=" << asdInfo->ipAddress_ << ":" << asdInfo->port_;

  while (!asdInfo->stopping_) 
  {
    GatewayMsg anyReq;
    auto ret = asdInfo->queue_->read(anyReq);
    assert(ret == 0);

    switch (anyReq.opcode_) {

      case Opcode::OPEN:
        {
          LOG(INFO) << "got open from edge process=" << anyReq.edgePid_;
          auto newEdge = std::make_shared<EdgeQueue>(anyReq.edgePid_);
          assert(newEdge->pid_ == anyReq.edgePid_);
          edges_.insert(newEdge);
          break;
        }
      case Opcode::READ:
        {
          const int pid = (pid_t)anyReq.edgePid_;
          auto edgePtr = edges_.find(pid);
          
          if (edgePtr) {
            LOG(INFO) << "got read from pid=" << pid << " for file=" << anyReq.filename_;

            giocb* iocb = new giocb; // freed on io completion
            edgePtr->giocb_from_GatewayMsg(*iocb, anyReq);

            auto aio_ret = aio_read(asdInfo->ctx_, iocb);
            if (aio_ret != 0) {
              anyReq.retval_ = -1;
              anyReq.errval_ = EIO;
              auto ret = edgePtr->write(anyReq);
              assert(ret == 0);
              delete iocb;
            } else  {
              aio_wait_all(asdInfo->ctx_);
            }
          } else {
            LOG(ERROR) << " could not find queue for pid=" << pid;
          }
          break;
        }
      case Opcode::CLOSE:
        {
          LOG(INFO) << "got close from edge process=" << anyReq.edgePid_;
          int ret = edges_.drop(anyReq.edgePid_);
          (void) ret;
          break;
        }
      default:
        break;
    }
  }

  asdInfo->stopped_ = true;
  LOG(INFO) << "stopped asd thread=" << gettid() << " for uri=" << asdInfo->ipAddress_ << ":" << asdInfo->port_;
  return 0;
}

int RoraGateway::handleReadCompletion(int fd, uintptr_t ptr) {

  uint64_t doneCount =0; // move to ASDInfo

  ASDInfo* asdPtr = reinterpret_cast<ASDInfo*>(ptr);

  std::vector<giocb*> iocb_vec;
  int times = 1; // ignored right now
  int r = aio_getevents(asdPtr->ctx_, times, iocb_vec);

  if (r == 0) {
    for (auto& iocb : iocb_vec) {

      int pid = (int) iocb->user_ctx;

      auto edgeIter = edges_.catalog_.find(pid);
      if (edgeIter != edges_.catalog_.end()) {
        auto edgeQueue = edgeIter->second;

        GatewayMsg respMsg;
        edgeQueue->GatewayMsg_from_giocb(respMsg, *iocb, 
            aio_return(iocb), aio_error(iocb));
        LOG(INFO) << "send response to pid=" << pid 
          << " for filename=" << iocb->filename;
        auto ret = edgeQueue->write(respMsg);
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

  for (auto& asdPtr : asdList_) {
    asdPtr->thread_ = std::thread(std::bind(&RoraGateway::asdThreadFunc, this, asdPtr.get()));
  }

  edges_.thread_ = std::thread(std::bind(&RoraGateway::responseThreadFunc, this));

  edges_.thread_.join();

  // TODO wont exit right now because its stuck in message queue read
  for (auto& asdPtr : asdList_) {
    asdPtr->thread_.join();
  }

  return ret;
}

int RoraGateway::shutdown() {
  int ret = 0;

  edges_.stopping_ = true;
  edges_.epoller_.shutdown();

  for (auto& asdPtr : asdList_) {
    asdPtr->stopping_ = true;
  }

  return ret;
}

}
}
