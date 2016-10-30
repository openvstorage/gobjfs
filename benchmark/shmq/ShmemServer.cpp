#include <rora/GatewayProtocol.h>
#include <rora/EdgeQueue.h>

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

int main(int argc, char* argv[])
{
  int ret = 0;

  int pid = atoi(argv[1]);

  edgeQueue = gobjfs::make_unique<EdgeQueue>(pid);

  Timer throughputTimer(true);

  size_t doneCount = 0;

  for (doneCount = 0; doneCount < 1000; doneCount ++) {
    GatewayMsg responseMsg;
    const auto ret = edgeQueue->read(responseMsg);
    assert(ret == 0);
  }

  int64_t timeMilli = throughputTimer.elapsedMilliseconds();
  float iops = ((float)doneCount * 1000) / timeMilli;

  edgeQueue.reset();
  std::cout << "iops=" << iops << std::endl;
}
