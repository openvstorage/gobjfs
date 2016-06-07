#include "NetworkXioInterface.h"
#include "NetworkXioServer.h"
#include <string>
#include <assert.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <iostream>
#include <assert.h>
#include <vector>
#include <string.h>
#include <math.h>
#include <chrono>
#include <sys/time.h>

#include <mutex>
#include <atomic>
#include <condition_variable>
#include <thread>
#include <boost/program_options.hpp>
#include <fstream>
#include <gobj.h>
#include <strings.h>
#include <ObjectFS.h>

using namespace boost::program_options;
using namespace volumedriverfs;
using namespace std;

struct Config {
  size_t  objectSz = 16384;
  size_t  alignSz = 4096;
  size_t  objectCacheSz = 1000;

  int32_t  totalWrIOps = 10000;
  int32_t totalRdIOps = 10000;
  int32_t fillData = 0;
  int32_t maxWriterThreads = 1;
  int32_t maxReaderThreads = 1;
  size_t  containerSz = 1073741824;
  size_t  containerMetaSz = 4096;
  size_t  segmentSz = 16384;
  std::string ioConfigFile = {"/etc/dcengines/TestObjectFS.conf"};
  std::string device = {"/dev/loop2"};

  Config()
  {
  }

  void
  readConfig(const std::string& configFileName)
  {
    options_description desc("allowed options");
    desc.add_options()
      ("object_sz", value<size_t>(&objectSz)->required(), "obejct size for reads & writes")
      ("align_sz", value<size_t>(&alignSz)->required(), "size that memory is aligned to ")
      ("object_cache_sz", value<size_t>(&objectCacheSz)->required(), "Object Cache Size - buffered write objects")
      ("total_read_iops", value<int32_t>(&totalRdIOps)->required(), "Number of Read IOPs")
      ("total_write_iops", value<int32_t>(&totalWrIOps)->required(), "Number of Write IOPs")
      ("fill_data", value<int32_t>(&fillData)->required(), "Only for read operations, fill specified number of data")
      ("max_writer_threads", value<int32_t>(&maxWriterThreads)->required(), "Number of Writer Threads")
      ("max_reader_threads", value<int32_t>(&maxReaderThreads)->required(), "Number of Writer Threads")
      ("container_sz", value<size_t>(&containerSz)->required(), "Containers of size disk space divided into")
      ("container_meta_sz", value<size_t>(&containerMetaSz)->required(), "Container Metadata Size")
      ("segment_sz", value<size_t>(&segmentSz)->required(), "Segments of size a container space divided into")
      ("ioconfig_file", value<std::string>(&ioConfigFile)->required(), "IOExecutor config file")
      ("device", value<std::string>(&device)->required(), "Device file path")
            ;
    std::ifstream configFile(configFileName);
    variables_map vm;
    store(parse_config_file(configFile, desc), vm);
    notify(vm);

    LOG(INFO)
      << "=================================================================" << std::endl
      << "     BenchObjectFSWriter.conf" << std::endl
      << "=================================================================" << std::endl;
    std::ostringstream s;
    for (const auto& it : vm) {
      s << it.first.c_str() << "=";
      auto& value = it.second.value();
      if (auto v = boost::any_cast<size_t>(&value))
        s << *v << std::endl;
      else if (auto v = boost::any_cast<int32_t>(&value))
        s << *v << std::endl;
      else if (auto v = boost::any_cast<std::string>(&value))
        s << *v << std::endl;
      else if (auto v = boost::any_cast<bool>(&value))
        s << *v << std::endl;
      else
        s << "error" << std::endl;
    }
    LOG(INFO) << s.str();
    LOG(INFO)
      << "==================================================================" << std::endl;
  }
};

Config    config;
int main(int argc, char *argv[]) {
    string Url = "tcp://127.0.0.1:21321";
    // log files are in /tmp
    google::InitGoogleLogging(argv[0]);
    
    if (argc == 2) {
        config.readConfig(argv[1]);
    }
    //Config config;
    
    //TODO Read COnfig file
    
    if (ObjectFS_Init(config.ioConfigFile.c_str()) < 0) {
        LOG(ERROR) << "ObjectFS_init Failed";
        return -1;
    }
    NetworkXioServer *xs = new NetworkXioServer(Url);
    xs->run();
}
