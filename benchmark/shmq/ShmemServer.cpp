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

std::map<int, EdgeQueueUPtr> edgeQueueMap;
ASDQueueUPtr asdQueue;
size_t maxQueueLen = 256;
const std::string asdQueueName = "bench_asd_queue";

int main(int argc, char* argv[])
{
  int ret = 0;

  asdQueue = gobjfs::make_unique<ASDQueue>(asdQueueName, maxQueueLen, GatewayMsg::MaxMsgSize);

  while (1) {

    GatewayMsg reqMsg;
    ret = asdQueue->read(reqMsg);
    assert(ret == 0);

    switch (reqMsg.opcode_) { 

      case ADD_EDGE_REQ : 
        {
          auto edgeQueue = gobjfs::make_unique<EdgeQueue>(reqMsg.edgePid_);
      
          GatewayMsg respMsg;
          ret = edgeQueue->write(respMsg);
          assert(ret == 0);

          std::cout << "added edgeQueue for pid=" << reqMsg.edgePid_ << std::endl;
          edgeQueueMap.insert(std::make_pair(reqMsg.edgePid_, std::move(edgeQueue)));
          break;
        }
      case DROP_EDGE_REQ : 
        {
          auto iter = edgeQueueMap.find(reqMsg.edgePid_);
          assert(iter != edgeQueueMap.end());
          auto& edgeQueue = iter->second;

          GatewayMsg respMsg;
          ret = edgeQueue->write(respMsg);
          assert(ret == 0);

          edgeQueueMap.erase(iter);
          std::cout << "dropped edgeQueue for pid=" << reqMsg.edgePid_ << std::endl;
          break;
        }
      default : 
        {
          GatewayMsg respMsg;
          auto iter = edgeQueueMap.find(reqMsg.edgePid_);
          assert(iter != edgeQueueMap.end());
          auto& edgeQueue = iter->second;
          ret = edgeQueue->write(respMsg);
          assert(ret == 0);
          break;
        }
    }
  }
}
