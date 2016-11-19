#include <rora/GatewayClient.h>
#include <rora/GatewayProtocol.h>

#include <util/Timer.h>
#include <util/Stats.h>
#include <util/os_utils.h>
#include <gobjfs_log.h>

#include <unistd.h>
#include <random>
#include <future>

#include <boost/program_options.hpp>

#include <boost/log/trivial.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/record_ostream.hpp>

namespace bpo = boost::program_options;
using namespace bpo;

using namespace gobjfs::rora;
using namespace gobjfs::os;
using namespace gobjfs::stats;

namespace logging = boost::log;
namespace src = boost::log::sources;
namespace sinks = boost::log::sinks;
namespace keywords = boost::log::keywords;

struct Config {
  int32_t roraVersion_ = 1;
  uint32_t blockSize = 4096;
  uint32_t maxFiles = 1000;
  uint32_t maxBlocks = 10000;
  uint64_t maxIO = 10000;
  uint64_t maxOutstandingIO = 1;
  uint32_t runTimeSec = 1;
  bool doMemCheck = false;
  uint32_t shortenFileSize = 0;
  std::vector<std::string> dirPrefix;
  std::vector<std::string> ipAddressVec;
  std::vector<std::string> transportVec;
  std::vector<int> portVec;

  std::string readStyle;
  int readStyleAsInt = -1;

  int readConfig(const std::string &configFileName) {
    options_description desc("allowed options");
    desc.add_options()
        ("rora_version", value<int32_t>(&roraVersion_)->default_value(1), "version of rora gateway to connect to")
        ("block_size", value<uint32_t>(&blockSize)->required(), "blocksize for reads & writes")
        ("num_files", value<uint32_t>(&maxFiles)->required(), "number of files") 
        ("max_file_blocks", value<uint32_t>(&maxBlocks)->required(), "number of [blocksize] blocks in file")
        ("max_io", value<uint64_t>(&maxIO)->required(), "number of ops to execute")
        ("max_outstanding_io", value<uint64_t>(&maxOutstandingIO)->required(), "max outstanding io")
        ("run_time_sec", value<uint32_t>(&runTimeSec)->required(), "number of secs to run")
        ("do_mem_check", value<bool>(&doMemCheck)->required(), "compare read buffer")
        ("shorten_file_size", value<uint32_t>(&shortenFileSize), "shorten the size by this much to test nonaliged reads")
        ("mountpoint", value<std::vector<std::string>>(&dirPrefix)->required()->multitoken(), "ssd mount point")
        ("ipaddress", value<std::vector<std::string>>(&ipAddressVec)->required(), "ip address")
        ("port", value<std::vector<int>>(&portVec)->required(), "port on which asd server running")
        ("transport", value<std::vector<std::string>>(&transportVec)->required(), "tcp or rdma")
          ;

    std::ifstream configFile(configFileName);
    variables_map vm;
    store(parse_config_file(configFile, desc), vm);
    notify(vm);

    if (runTimeSec && maxIO) {
      LOG(INFO) << "run_time_sec and per_thread_io both defined.  Benchmark will be run based on run_time_sec";
      maxIO = 0;
    } else if (maxIO % maxOutstandingIO != 0) {
      // truncate it so that read does not terminate with outstanding IO
      maxIO -= (maxIO % maxOutstandingIO);
      LOG(INFO) << "reduced maxIO to " << maxIO << " to keep it multiple of " << maxOutstandingIO;
    }

    LOG(INFO)
        << "================================================================="
        << std::endl << "     EdgeClient config" << std::endl
        << "================================================================="
        << std::endl;
    std::ostringstream s;
    for (const auto &it : vm) {
      s << it.first.c_str() << "=";
      auto &value = it.second.value();
      if (auto v = boost::any_cast<uint64_t>(&value))
        s << *v << std::endl;
      else if (auto v = boost::any_cast<uint32_t>(&value))
        s << *v << std::endl;
      else if (auto v = boost::any_cast<std::string>(&value))
        s << *v << std::endl;
      else if (auto v = boost::any_cast<bool>(&value))
        s << *v << std::endl;
      else if (auto v = boost::any_cast<std::vector<int>>(&value)) {
        for (auto val : *v) {
          s << val << ",";
        }
        s << std::endl;
      } else if (auto v = boost::any_cast<std::vector<std::string>>(&value)) {
        for (auto val : *v) {
          s << val << ",";
        }
        s << std::endl;
      } else
        s << "cannot interpret value " << std::endl;
    }
    LOG(INFO) << s.str();
    LOG(INFO)
        << "=================================================================="
        << std::endl;

    return 0;
  }
};

static Config config;

// =====================

struct FileManager {

  std::string buildFileName(uint64_t fileNum) {
    return "/bench" + std::to_string(fileNum) + ".data";
  }
  
  std::string getFilename(uint64_t fileNum) {
    static const auto numDir = config.dirPrefix.size();
    return config.dirPrefix[fileNum % numDir] + buildFileName(fileNum);
  }
};

static FileManager fileMgr;
// =====================

// record run identifier and number of concurrent edge processes, 
// for purpose of benchmark - for easier grep thru text files
static std::string runIdentifier; 
static int numEdgeProcesses = 0;

// =====================

struct RunContext {

  StatsCounter<uint64_t> readLatency;
  uint64_t failedReads{0};
  uint64_t iops{0};
  Timer throughputTimer;

  size_t maxIO{0};
  size_t doneCount{0};
  size_t progressCount{0};
  bool mustExit{false};

  RunContext() {
    maxIO = config.maxIO;
  }

  void start() {
    throughputTimer.reset();
  }

  void finalize() {
    checkTermination();
    int64_t timeMilli = throughputTimer.elapsedMilliseconds();
    iops = (doneCount * 1000) / timeMilli;

    std::ostringstream s;
    s 
      << "runid=" << runIdentifier
      << ",num_edges=" << numEdgeProcesses
      << ":batch_size=" << config.maxOutstandingIO
      << ":num_io=" << doneCount
      << ":time(msec)=" << timeMilli
      << ":iops=" << iops 
      << ":failed_reads=" << failedReads
      << ":read_latency(usec)=" << readLatency
      << std::endl;
  
    LOG(INFO) << s.str();
  }

  bool isFinished() {
    if (config.maxIO) {
      // this is a max io-based run
      if (doneCount == maxIO) {
        mustExit = true;
      }
    } else {
      // this is a time-based run
      maxIO = doneCount;
    }
    return mustExit;
  }

  void incrementCount(bool hasFailed) {
    if (hasFailed) {
      failedReads ++;
    }
    doneCount ++;
    progressCount ++;
    if (progressCount == maxIO/10) {
      LOG(INFO) << "thread=" << gettid() 
        << " done percent=" << ((doneCount * 100)/maxIO);
      progressCount = 0;
    }
  }

  void checkTermination() {
    assert(doneCount == maxIO);
    assert(doneCount > 0); // must have run
  }

  void doRandomRead(GatewayClient* gc);
};

void RunContext::doRandomRead(GatewayClient* gc) {

  std::mt19937 seedGen(getpid() + gobjfs::os::GetCpuCore());
  std::uniform_int_distribution<decltype(config.maxFiles)> filenumGen(
      0, config.maxFiles - 1);
  std::uniform_int_distribution<decltype(config.maxBlocks)> blockGenerator(
      0, config.maxBlocks - 1);

  start();

  while (!isFinished()) {

    eioRequest submitReq;
    submitReq.asdIdx_ = 0;
    // send read msg 
    // a batch may contains read offset for different files
    for (size_t batchIdx = 0; batchIdx < config.maxOutstandingIO; batchIdx ++) {
      eiocb *iocb = new eiocb;
      const uint32_t fileNumber = filenumGen(seedGen);
      iocb->filename_ = fileMgr.getFilename(fileNumber);
      iocb->offset_ = blockGenerator(seedGen) * config.blockSize;
      iocb->size_ = config.blockSize;
      submitReq.eiocbVec_.push_back(std::unique_ptr<eiocb>(iocb));
    }

    Timer latencyTimer(true); // one timer for all batch

    auto ret = gc->asyncRead(submitReq);
    assert(ret == 0);
  
    for (size_t batchIdx = 0; batchIdx < config.maxOutstandingIO; batchIdx ++) {
      // get read response
      eioRequest completedReq;
      ret = gc->waitForResponse(completedReq);
      assert(ret == 0);

      // check retval, errval, filename, offset, size match
      if (config.doMemCheck) {
      }
  
      for (size_t idx = 0; idx < completedReq.eiocbVec_.size(); idx ++) {
        incrementCount(completedReq.retvalVec_[idx] < 0);
        // free allocated shared segment
      }
      gc->release(completedReq);
    }

    readLatency = latencyTimer.elapsedMicroseconds();
  }

  finalize();
}

int main(int argc, char* argv[])
{
  if (argc < 3) {
    std::cout << argv[0] << " <runIdentifier(string)> <number of concurrent edge processes>" << std::endl;
    exit(1);
  }
  runIdentifier = argv[1];
  numEdgeProcesses = atoi(argv[2]);

  namespace logging = boost::log;
  logging::core::get()->set_filter(logging::trivial::severity >=
      logging::trivial::info);

  std::string logFileName(argv[0]);
  logFileName += std::to_string(getpid()) + std::string("_%N.log");
  logging::add_file_log
  (
    keywords::file_name = logFileName,
    keywords::rotation_size = 10 * 1024 * 1024,
    keywords::time_based_rotation = sinks::file::rotation_at_time_point(0, 0, 0), 
    keywords::auto_flush = true,
    keywords::format = "[%TimeStamp%]: %Message%"
  );

  logging::add_common_attributes();// puts timestamp in log

  std::cout << "logs in " << logFileName << std::endl;

  int ret = 0;

  std::string configFileName = "./edge_process.conf";
  if (argc > 3) {
    configFileName = argv[3];
  }
  ret = config.readConfig(configFileName);
  assert(ret == 0);

  GatewayClient gc(config.roraVersion_, config.maxOutstandingIO,
      config.blockSize);

  // then ask gateway to add asds
  const size_t numASD = config.transportVec.size();
  for (size_t idx = 0; idx < numASD; idx ++) {

    ret = gc.addASD(config.transportVec[idx],
      config.ipAddressVec[idx],
      config.portVec[idx]);
    assert(ret == 0);

  }

  RunContext r;
  auto fut = std::async(std::launch::async, std::bind(&RunContext::doRandomRead, &r, &gc));

  if (config.runTimeSec) {
    // print progress count here for time-based run
    for (int idx = 0; idx < 10; idx++) {
      sleep(config.runTimeSec/10);
      LOG(INFO) << " done percent=" << (idx + 1) * 10;
    }
    r.mustExit = true;
  }

  fut.wait();

  gc.shutdown();
}
