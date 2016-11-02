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
const std::string asdQueueName = "bench_asd_queue";

void asdThreadFunc() {
  int ret = 0;

  std::cout << "started thread=" << gettid() << std::endl;

  while (1) {

    GatewayMsg reqMsg;
    ret = asdQueue->read(reqMsg);
    assert(ret == 0);

    switch (reqMsg.opcode_) { 

      case ADD_EDGE_REQ : 
        {
          auto edgeQueue = gobjfs::make_unique<EdgeQueue>(reqMsg.edgePid_);
          auto edgeQueuePtr = edgeQueue.get();
      
          {
            std::unique_lock<std::mutex> l(edgeQueueMutex);
            edgeQueueMap.insert(std::make_pair(reqMsg.edgePid_, std::move(edgeQueue)));
          }

          std::cout << "added edgeQueue for pid=" << reqMsg.edgePid_ << std::endl;

          GatewayMsg respMsg;
          ret = edgeQueuePtr->write(respMsg);
          assert(ret == 0);

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
          ret = edgeQueue->write(respMsg);
          assert(ret == 0);

          {
            std::unique_lock<std::mutex> l(edgeQueueMutex);
            edgeQueueMap.erase(iter);
          }
          std::cout << "dropped edgeQueue for pid=" << reqMsg.edgePid_ << std::endl;
          break;
        }
      default : 
        {
          decltype(edgeQueueMap)::iterator iter;
          {
            std::unique_lock<std::mutex> l(edgeQueueMutex);
            iter = edgeQueueMap.find(reqMsg.edgePid_);
          }
          assert(iter != edgeQueueMap.end());
          auto& edgeQueue = iter->second;

          GatewayMsg respMsg;
          ret = edgeQueue->write(respMsg);
          assert(ret == 0);
          break;
        }
    }
  }
}

int main(int argc, char* argv[])
{
  asdQueue = gobjfs::make_unique<ASDQueue>(asdQueueName, maxQueueLen, GatewayMsg::MaxMsgSize);

  std::vector<std::future<void>> futVec;

  for (int i = 0; i < 8; i++) {
    auto fut = std::async(std::launch::async, asdThreadFunc);
    futVec.push_back(std::move(fut));
  }

  for (auto& fut : futVec) {
    fut.wait();
  }

}
