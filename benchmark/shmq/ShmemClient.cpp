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

EdgeQueueUPtr edgeQueue;
ASDQueueUPtr asdQueue;
const std::string asdQueueName = "bench_asd_queue";
size_t maxAllocSize = 4096;
size_t maxQueueLen = 10;
StatsCounter<int64_t> latMilli;

int main(int argc, char* argv[])
{
  int ret = 0;

  int totalCount = 10000;
  if (argc > 1) {
    totalCount = atoi(argv[1]);
  }


  int pid = getpid();
  LOG(INFO) << "pid=" << pid << std::endl;

  // create new for this process
  edgeQueue = gobjfs::make_unique<EdgeQueue>(pid, maxQueueLen,
      GatewayMsg::MaxMsgSize, maxAllocSize);

  asdQueue = gobjfs::make_unique<ASDQueue>("1.0", "tcp", "localhost", 12321);

  {
    GatewayMsg reqMsg;
    reqMsg.edgePid_ = pid;
    reqMsg.opcode_ = ADD_EDGE_REQ;
    ret = asdQueue->write(reqMsg);
    assert(ret == 0);

    GatewayMsg respMsg;
    ret = edgeQueue->readResponse(respMsg);
    assert(ret == 0);
  }

  Timer throughputTimer(true);

  size_t doneCount = 0;

  for (doneCount = 0; doneCount < totalCount; doneCount ++) {

    Timer latencyTimer;

    if (doneCount % 100 == 0) {
      latencyTimer.reset();
    }

    GatewayMsg reqMsg;
    reqMsg.edgePid_ = pid;
    ret = asdQueue->write(reqMsg);
    assert(ret == 0);

    GatewayMsg respMsg;
    ret = edgeQueue->readResponse(respMsg);
    assert(ret == 0);

    if (doneCount % 100 == 0) {
      latMilli = latencyTimer.elapsedMicroseconds();
    }
  }

  int64_t timeMilli = throughputTimer.elapsedMilliseconds();
  float iops = ((float)doneCount * 1000) / timeMilli;

  {
    GatewayMsg reqMsg;
    reqMsg.edgePid_ = pid;
    reqMsg.opcode_ = DROP_EDGE_REQ;
    ret = asdQueue->write(reqMsg);
    assert(ret == 0);

    GatewayMsg respMsg;
    ret = edgeQueue->readResponse(respMsg);
    assert(ret == 0);
  }

  edgeQueue.reset();

  LOG(INFO) << "iops=" << iops << ",lat=" << latMilli << std::endl;
}
