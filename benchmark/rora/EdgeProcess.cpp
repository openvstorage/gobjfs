#include <rora/GatewayProtocol.h>
#include <rora/EdgeQueue.h>
#include <rora/ASDQueue.h>
#include <unistd.h>
#include <glog/logging.h>

#include <boost/program_options.hpp>

namespace bpo = boost::program_options;
using namespace bpo;

using namespace gobjfs::rora;

struct Config {
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
    }

    if (maxIO % maxOutstandingIO != 0) {
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

int main(int argc, char* argv[])
{
  int pid = getpid();

  std::string configFileName = "./edge_process.conf";
  if (argc > 1) {
    configFileName = argv[1];
  }
  auto ret = config.readConfig(configFileName);
  assert(ret == 0);

  std::vector<ASDQueueUPtr> asdQueueVec;

  // create new for this process
  EdgeQueueUPtr edgeQueue = gobjfs::make_unique<EdgeQueue>(pid, config.maxOutstandingIO, 
      GatewayMsg::MaxMsgSize, config.blockSize);

  // open existing
  const size_t numASD = config.transportVec.size();
  for (size_t idx = 0; idx < numASD; idx ++) {
    std::string uri = config.ipAddressVec[idx] + ":" + std::to_string(config.portVec[idx]);
    auto asdPtr = gobjfs::make_unique<ASDQueue>(uri);
    asdQueueVec.push_back(std::move(asdPtr));
  }

  ASDQueue* asdQueue = asdQueueVec[0].get();
  {
    // sending open message will cause rora gateway to open
    // the EdgeQueue for sending responses
    auto ret = asdQueue->write(createOpenRequest());
    assert(ret == 0);
  }

  for (size_t idx = 0; idx < config.maxIO; idx ++) {
    // send read msg
    for (size_t batchIdx = 0; batchIdx < config.maxOutstandingIO; batchIdx ++) {
      off_t offset = 0;
      auto ret = asdQueue->write(createReadRequest(edgeQueue.get(), "abcd", 
        batchIdx * config.blockSize, 
        config.blockSize));
      assert(ret == 0);
    }
  
    for (size_t batchIdx = 0; batchIdx < config.maxOutstandingIO; batchIdx ++) {
      // get read response
      GatewayMsg responseMsg;
      auto ret = edgeQueue->read(responseMsg);
      assert(ret == 0);
  
      // check retval, errval, filename, offset, size match
      if (config.doMemCheck) {
      }
  
      responseMsg.rawbuf_ = edgeQueue->segment_->get_address_from_handle(responseMsg.buf_);

      // free allocated shared segment
      edgeQueue->free(responseMsg.rawbuf_);
      responseMsg.rawbuf_ = nullptr;
      responseMsg.buf_ = 0;
    }
  }

  {
    // sending close message will cause rora gateway to close
    // the EdgeQueue for sending responses
    auto ret = asdQueue->write(createCloseRequest());
    assert(ret == 0);
  }

  asdQueueVec.clear();
  edgeQueue.reset();
}
