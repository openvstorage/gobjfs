#include <rora/GatewayProtocol.h>
#include <rora/EdgeQueue.h>
#include <rora/ASDQueue.h>

#include <util/Timer.h>
#include <util/Stats.h>
#include <util/os_utils.h>
#include <gobjfs_log.h>

#include <unistd.h>
#include <random>
#include <future>

using namespace gobjfs::rora;
using namespace gobjfs::os;
using namespace gobjfs::stats;

std::mutex edgeQueueMutex;
std::map<int, EdgeQueueUPtr> edgeQueueMap;
ASDQueueUPtr asdQueue;
const size_t maxQueueLen = 256;
const size_t maxBatchSize = 8; // same as IOV_LEN in xio
const std::string asdQueueName = "bench_asd_queue";

void asdThreadFunc() {

  size_t cond1_count = 0;
  size_t cond2_count = 0;
  size_t total_count = 0;
  StatsCounter<int64_t> subSize;

  int ret = 0;

  LOG(INFO) << "started thread=" << gettid() << std::endl;

  std::vector<int> pidVec;
  int idle = 0;
  size_t actualBatchSize = maxBatchSize;

  while (1) {

    GatewayMsg reqMsg;
    if (pidVec.size()) {
      ret = asdQueue->try_read(reqMsg);
      if (ret != 0) {
        idle ++;
      } 
    } else {
      ret = asdQueue->read(reqMsg);
      assert(ret == 0);
    }

    if (ret == 0) {
      switch (reqMsg.opcode_) { 

        case ADD_EDGE_REQ : 
          {
            auto edgeQueue = gobjfs::make_unique<EdgeQueue>(reqMsg.edgePid_);
            auto edgeQueuePtr = edgeQueue.get();
        
            {
              std::unique_lock<std::mutex> l(edgeQueueMutex);
              edgeQueueMap.insert(std::make_pair(reqMsg.edgePid_, std::move(edgeQueue)));
            }

            LOG(INFO) << "added edgeQueue for pid=" << reqMsg.edgePid_ << std::endl;

            GatewayMsg respMsg;
            ret = edgeQueuePtr->writeResponse(respMsg);
            assert(ret == 0);

	    actualBatchSize = std::min(maxBatchSize, edgeQueueMap.size());

            break;
          }
        case DROP_EDGE_REQ : 
          {
            decltype(edgeQueueMap)::iterator iter;
            {
              std::unique_lock<std::mutex> l(edgeQueueMutex);
              iter = edgeQueueMap.find(reqMsg.edgePid_);
            }
            assert(iter != edgeQueueMap.end());
            auto& edgeQueue = iter->second;

            GatewayMsg respMsg;
            ret = edgeQueue->writeResponse(respMsg);
            assert(ret == 0);

            {
              std::unique_lock<std::mutex> l(edgeQueueMutex);
              edgeQueueMap.erase(iter);
            }
	    actualBatchSize = std::min(maxBatchSize, edgeQueueMap.size());
            LOG(INFO) << "dropped edgeQueue for pid=" << reqMsg.edgePid_ << std::endl;
            break;
          }
        default : 
          {
            pidVec.push_back(reqMsg.edgePid_);
          }
      } // switch
    } // if

    bool cond1 = ((idle >= 1) && pidVec.size());
    bool cond2 = (pidVec.size() >= actualBatchSize);
    if (cond1) {
      cond1_count ++;
    } 
    if (cond2) {
      cond2_count ++;
    }
    if (cond1 || cond2) {
    
      total_count ++;
      subSize = pidVec.size();
      if (total_count >= 1000) {
        LOG(INFO) << "batch=" << subSize << ":idle=" << cond1_count << ":full=" << cond2_count << std::endl;
        total_count = 0;
        cond1_count = 0;
        cond2_count = 0;
        subSize.reset();
      }

      for (auto pid : pidVec) {

        decltype(edgeQueueMap)::iterator iter;
        {
          std::unique_lock<std::mutex> l(edgeQueueMutex);
          iter = edgeQueueMap.find(pid);
        }
        assert(iter != edgeQueueMap.end());
        auto& edgeQueue = iter->second;
  
        GatewayMsg respMsg;
        ret = edgeQueue->writeResponse(respMsg);
        assert(ret == 0);
      }
  
      if (idle == 1) {
        idle = 0;
      }
      pidVec.clear();
    }
  } // while
}

int main(int argc, char* argv[])
{
  asdQueue = gobjfs::make_unique<ASDQueue>("1.0", "tcp", "localhost", 12321, maxQueueLen, GatewayMsg::MaxMsgSize);

  std::vector<std::future<void>> futVec;

  for (int i = 0; i < 8; i++) {
    auto fut = std::async(std::launch::async, asdThreadFunc);
    futVec.push_back(std::move(fut));
  }

  for (auto& fut : futVec) {
    fut.wait();
  }

}
