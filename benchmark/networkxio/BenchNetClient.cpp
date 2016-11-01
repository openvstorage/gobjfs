/*
Copyright (C) 2016 iNuron NV

This file is part of Open vStorage Open Source Edition (OSE), as available from


    http://www.openvstorage.org and
    http://www.openvstorage.com.

This file is free software; you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
as published by the Free Software Foundation, in version 3 as it comes
in the <LICENSE.txt> file of the Open vStorage OSE distribution.

Open vStorage is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY of any kind.
*/

#include <util/CpuStats.h>
#include <util/SemaphoreWrapper.h>
#include <util/Stats.h>
#include <util/Timer.h>
#include <util/os_utils.h>

#include <gobjfs_log.h>
#include <gobjfs_client.h>
#include <networkxio/gobjfs_client_common.h>

#include <boost/program_options.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/record_ostream.hpp>

#include <fstream>
#include <iostream>
#include <deque>
#include <sstream>

#include <sys/stat.h>
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#include <string.h> //basename
#endif
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <random>
#include <future>

using namespace boost::program_options;
using namespace gobjfs::xio;
using namespace gobjfs::os;
using gobjfs::stats::StatsCounter;
using gobjfs::stats::Timer;

namespace logging = boost::log;
namespace src = boost::log::sources;
namespace sinks = boost::log::sinks;
namespace keywords = boost::log::keywords;

struct Config {
  uint32_t blockSize = 4096;
  uint32_t alignSize = 4096;

  uint32_t maxFiles = 1000;
  uint32_t maxBlocks = 10000;
  uint64_t perThreadIO = 10000;
  uint32_t runTimeSec = 1;
  uint64_t maxOutstandingIO = 1;
  bool doMemCheck = false;
  uint32_t maxThr = 1;
  uint32_t shortenFileSize = 0;
  std::vector<std::string> dirPrefix;
  std::string ipAddress;
  std::string transport;
  int port = 0;

  std::string readStyle;
  int readStyleAsInt = -1;

  int readConfig(const std::string &configFileName) {
    options_description desc("allowed options");
    desc.add_options()
        ("block_size", value<uint32_t>(&blockSize)->required(), "blocksize for reads & writes")
        ("align_size", value<uint32_t>(&alignSize)->required(), "size that memory is aligned to ")
        ("num_files", value<uint32_t>(&maxFiles)->required(), "number of files") 
        ("max_file_blocks", value<uint32_t>(&maxBlocks)->required(), "number of [blocksize] blocks in file")
        ("per_thread_max_io", value<uint64_t>(&perThreadIO)->required(), "number of ops to execute")
        ("run_time_sec", value<uint32_t>(&runTimeSec)->required(), "number of secs to run")
        ("max_outstanding_io", value<uint64_t>(&maxOutstandingIO)->required(), "max outstanding io")
        ("read_style", value<std::string>(&readStyle)->required(), "aio_read or aio_readv")
        ("max_threads", value<uint32_t>(&maxThr)->required(), "max threads")
        ("shorten_file_size", value<uint32_t>(&shortenFileSize), "shorten the size by this much to test nonaliged reads")
        ("mountpoint", value<std::vector<std::string>>(&dirPrefix)->required()->multitoken(), "ssd mount point")
        ("ipaddress", value<std::string>(&ipAddress)->required(), "ip address")
        ("port", value<int>(&port)->required(), "port on which xio server running")
        ("transport", value<std::string>(&transport)->required(), "tcp or rdma")
        ("do_mem_check", value<bool>(&doMemCheck)->required(), "compare read buffer")
          ;

    std::ifstream configFile(configFileName);
    variables_map vm;
    store(parse_config_file(configFile, desc), vm);
    notify(vm);


    if (readStyle == "aio_read") {
      readStyleAsInt = 0;
    } else if (readStyle == "aio_readv") {
      readStyleAsInt = 1;
    } else {
      LOG(ERROR) << "read_style is invalid";
      return -1;
    }

    if (runTimeSec) {
      LOG(INFO) << "run_time_sec and per_thread_io both defined.  Benchmark will be run based on run_time_sec";
      perThreadIO = 0;
    }

    if (perThreadIO % maxOutstandingIO != 0) {
      // truncate it so that aio_read and readv do not terminate with outstanding IO
      perThreadIO -= (perThreadIO % maxOutstandingIO);
      LOG(INFO) << "reduced perThreadIO to " << perThreadIO << " to keep it multiple of " << maxOutstandingIO;
    }


    LOG(INFO)
        << "================================================================="
        << std::endl << "     BenchNetClient config" << std::endl
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
      else if (auto v = boost::any_cast<int>(&value))
        s << *v << std::endl;
      else if (auto v = boost::any_cast<std::string>(&value))
        s << *v << std::endl;
      else if (auto v = boost::any_cast<bool>(&value))
        s << *v << std::endl;
      else if (auto v = boost::any_cast<std::vector<std::string>>(&value)) {
        for (auto dirName : *v) {
          s << dirName << ",";
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

struct FixedSizeFileManager {

  void init() {}

  std::string buildFileName(uint64_t fileNum) {
    return "/bench" + std::to_string(fileNum) + ".data";
  }

  std::string getFilename(uint64_t fileNum) {
    static const auto numDir = config.dirPrefix.size();
    return config.dirPrefix[fileNum % numDir] + buildFileName(fileNum);
  }
};

SemaphoreWrapper startTogether;
FixedSizeFileManager fileMgr;

// benchmark info which being recorded and printed
struct BenchInfo {
  StatsCounter<uint64_t> readLatency;
  uint64_t failedReads{0};
  uint64_t iops{0};
};

BenchInfo globalBenchInfo;

struct ThreadCtx {

  int index_ = -1;

  uint32_t minFiles; 
  uint32_t maxFiles;
  uint32_t maxBlocks;

  client_ctx_attr_ptr ctx_attr_ptr;
  client_ctx_ptr ctx_ptr;

  uint64_t perThreadIO = 0;
  uint64_t doneCount = 0;
  uint64_t progressCount = 0;

  bool mustExit = false;

  std::vector<giocb *> iocb_vec;

  Timer throughputTimer;

  BenchInfo benchInfo;

  explicit ThreadCtx(int index,
    const Config& conf)
    : index_(index) {

    minFiles = 0;
    maxFiles = conf.maxFiles;
    maxBlocks = conf.maxBlocks;

    perThreadIO = conf.perThreadIO;

  }

  ~ThreadCtx() {
    //ctx_ptr.reset();
    ctx_attr_ptr.reset();
  }

  void checkTermination() {
    // check if all submitted IO is acked
    assert(iocb_vec.empty());
    assert(doneCount == perThreadIO);
  }

  bool isFinished() {
    if (config.perThreadIO) {
      if (doneCount == perThreadIO) {
        mustExit = true;
      }
    } else {
      perThreadIO = doneCount;
    }
    return mustExit;
  }

  void doRandomRead();
};

void ThreadCtx::doRandomRead() {

  std::mt19937 seedGen(getpid() + gobjfs::os::GetCpuCore());
  std::uniform_int_distribution<decltype(maxFiles)> filenumGen(
      minFiles, maxFiles - 1);
  std::uniform_int_distribution<decltype(maxBlocks)> blockGenerator(
      0, maxBlocks - 1);

  ctx_attr_ptr = ctx_attr_new();
  ctx_attr_set_transport(ctx_attr_ptr, config.transport, config.ipAddress, config.port);
  
  ctx_ptr = ctx_new(ctx_attr_ptr);
  assert(ctx_ptr);

  int err = ctx_init(ctx_ptr);
  assert(err == 0);

  startTogether.wakeup();
  startTogether.pause();

  throughputTimer.reset();

  gobjfs::os::CpuStats startCpuStats;
  startCpuStats.getThreadStats();

  while (!isFinished()) {

    char* rbuf = (char*) malloc(config.blockSize);
    assert(rbuf);


    giocb* iocb = new giocb;
    iocb->filename = fileMgr.getFilename(filenumGen(seedGen));
    iocb->aio_buf = rbuf;
    iocb->aio_offset = blockGenerator(seedGen) * config.blockSize; 
    iocb->aio_nbytes = config.blockSize;

    auto use_read = [&] () {

      Timer latencyTimer(true);

      auto ret = aio_read(ctx_ptr, iocb);

      if (ret != 0) {
        std::free(iocb->aio_buf);
        delete iocb;
        benchInfo.failedReads ++;
        //LOG(ERROR) << "failed0";
      } else {
      }

      if (doneCount % config.maxOutstandingIO == 0) {

        aio_wait_all(ctx_ptr);
        std::vector<giocb *> local_iocb_vec;
        ssize_t ret = aio_getevents(ctx_ptr, config.maxOutstandingIO, local_iocb_vec);

        for (auto &iocb : local_iocb_vec) {

          benchInfo.readLatency = latencyTimer.elapsedMicroseconds();

          if (ret != 0) {
            //LOG(ERROR) << "failed suspend " << ret;
            benchInfo.failedReads ++;      
          } else {
            ret = aio_return(iocb);
            if (ret != config.blockSize) {
              //LOG(ERROR) << "failed1 " << ret;
              benchInfo.failedReads ++;      
            }
          }
          aio_finish(iocb);
          std::free(iocb->aio_buf);
          delete iocb;
        }
        iocb_vec.clear();
      }
    };

    auto use_readv = [&] () {

      iocb_vec.push_back(iocb);

      if (doneCount % config.maxOutstandingIO == 0) {

        Timer latencyTimer(true);

        auto ret = aio_readv(ctx_ptr, iocb_vec);
        
        if (ret != 0) {
          benchInfo.failedReads += iocb_vec.size();
          //LOG(ERROR) << "failed2";
          for (auto &iocb : iocb_vec) {
            aio_finish(iocb);
            std::free(iocb->aio_buf);
            delete iocb;
          }
        } else {

          aio_wait_all(ctx_ptr);

          std::vector<giocb *> local_iocb_vec;
          aio_getevents(ctx_ptr, config.maxOutstandingIO, local_iocb_vec);

          benchInfo.readLatency = latencyTimer.elapsedMicroseconds();

          for (auto &iocb : local_iocb_vec) {
            ssize_t ret = aio_return(iocb);
            if (ret != config.blockSize) {
              benchInfo.failedReads ++;
              //LOG(ERROR) << "failed3";
            }
            aio_finish(iocb);
            std::free(iocb->aio_buf);
            delete iocb;
          }
        }
        iocb_vec.clear();
      }
    };

    // variety of ways to call async read are encoded as lambda func
    typedef std::function<void(void)> ReadFunc;
    ReadFunc funcs[] = { use_read, use_readv };

    // increment doneCount before ReadFunc call because it does batch 
    // ops internally when doneCount is multiple of maxOutstandingIO
    doneCount ++;

    funcs[config.readStyleAsInt]();

    progressCount ++;

    if (progressCount == perThreadIO/10) {
      LOG(INFO) << "thread=" << gettid() 
        << " index=" << index_ 
        << " work done percent=" << ((doneCount * 100)/perThreadIO);
      progressCount = 0;
    }
  }

  checkTermination();

  int64_t timeMilli = throughputTimer.elapsedMilliseconds();

  gobjfs::os::CpuStats endCpuStats;
  endCpuStats.getThreadStats();

  endCpuStats -= startCpuStats;

  // calc throughput
  benchInfo.iops = (doneCount * 1000 / timeMilli);

  std::ostringstream s;
  s << "thread=" << gobjfs::os::GetCpuCore() 
    << ":num_io=" << doneCount
    << ":time(msec)=" << timeMilli
    << ":iops=" << benchInfo.iops 
    << ":failed_reads=" << benchInfo.failedReads
    << ":read_latency(usec)=" << benchInfo.readLatency
    << ":xio stats=" << ctx_get_stats(ctx_ptr)
    << ":thread_stats=" << endCpuStats.ToString()
    << std::endl;

  LOG(INFO) << s.str();
}

static std::string configFileName = "bench_net_client.conf";

int main(int argc, char *argv[]) {
  // google::InitGoogleLogging(argv[0]); TODO logging
  //
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

  if (argc > 1) {
      configFileName = argv[1];
  }

  {
    struct stat statbuf;
    int err = stat(configFileName.c_str(), &statbuf);
    if (err != 0) {
      LOG(ERROR) << "need a config file " << configFileName << " in current dir";
      exit(1);
    }
  }
  int err = config.readConfig(configFileName);
  if (err != 0) {
    exit(1);
  }

  fileMgr.init();

  gobjfs::os::CpuStats startCpuStats;
  startCpuStats.getProcessStats();

  std::vector<ThreadCtx *> ctxVec;

  std::vector<std::future<void>> futVec;
  
  startTogether.init(config.maxThr, -1);

  for (decltype(config.maxThr) thr = 0; thr < config.maxThr; thr++) {
    ThreadCtx *ctx = new ThreadCtx(thr, config);
    auto f = std::async(std::launch::async, std::bind(&ThreadCtx::doRandomRead, ctx));

    futVec.emplace_back(std::move(f));
  }

  if (config.runTimeSec) {
    // wait till benchmark runs for N seconds
    sleep(config.runTimeSec);
    for (auto ctx : ctxVec) {
      ctx->mustExit = true;
    }
  }

  for (auto &elem : futVec) {
    elem.wait();
  }

  for (auto elem : ctxVec) {
    globalBenchInfo.readLatency += elem->benchInfo.readLatency;
    globalBenchInfo.failedReads += elem->benchInfo.failedReads;
    globalBenchInfo.iops += elem->benchInfo.iops;

    delete elem;
  }

  gobjfs::os::CpuStats endCpuStats;
  endCpuStats.getProcessStats();

  endCpuStats -= startCpuStats;

  {
    std::ostringstream s;

    s << ":num_threads=" << config.maxThr
      << ":iops=" << globalBenchInfo.iops
      << ":read_latency(usec)=" << globalBenchInfo.readLatency
      << ":failed_reads=" << globalBenchInfo.failedReads
      << ":process_stats=" << endCpuStats.ToString();

    LOG(INFO) << s.str();
  }

  {
    std::ostringstream s;
    // Print in csv format for easier input to excel
    s << "csv," 
      << config.maxThr << ","
      << globalBenchInfo.iops << "," 
      << globalBenchInfo.readLatency.mean() << "," 
      << endCpuStats.getCpuUtilization() << ","
      << "failed=" << globalBenchInfo.failedReads;

    LOG(INFO) << s.str() << std::endl;
  }
}
