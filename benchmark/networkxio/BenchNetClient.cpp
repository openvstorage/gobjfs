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
#include <util/ShutdownNotifier.h>
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

#include <fstream>
#include <iostream>
#include <sstream>

#include <dirent.h> // readdir
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/stat.h>
#include <sys/time.h> // gettimeofday
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

struct Config {
  uint32_t blockSize = 4096;
  uint32_t alignSize = 4096;

  uint32_t maxFiles = 1000;
  uint32_t maxBlocks = 10000;
  uint64_t perThreadIO = 10000;
  uint64_t maxOutstandingIO = 1;
  bool doMemCheck = false;
  uint32_t maxThr = 1;
  uint32_t shortenFileSize = 0;
  std::vector<std::string> dirPrefix;
  std::string ipAddress;
  std::string transport;
  int port = 0;

  int readConfig(const std::string &configFileName) {
    options_description desc("allowed options");
    desc.add_options()
        ("block_size", value<uint32_t>(&blockSize)->required(), "blocksize for reads & writes")
        ("align_size", value<uint32_t>(&alignSize)->required(), "size that memory is aligned to ")
        ("num_files", value<uint32_t>(&maxFiles)->required(), "number of files") 
        ("max_file_blocks", value<uint32_t>(&maxBlocks)->required(), "number of [blocksize] blocks in file")
        ("per_thread_max_io", value<uint64_t>(&perThreadIO)->required(), "number of ops to execute")
        ("max_outstanding_io", value<uint64_t>(&maxOutstandingIO)->required(), "max outstanding io")
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

static constexpr size_t FOURMB = (1 << 24);

struct FixedSizeFileManager {

  std::mutex mutex;
  uint32_t maxFiles_ = 0;

  void init(uint32_t MaxFiles) {
    maxFiles_ = MaxFiles;
  }

  std::string buildFileName(uint64_t fileNum) {
    return "/bench" + std::to_string(fileNum) + ".data";
  }

  int32_t parseFileName(std::string fileName, uint64_t &fileNum) {
    auto startPos = strlen("bench");
    auto endPos = fileName.find(".data");
    std::string aNumber = fileName.substr(startPos, endPos - startPos);
    std::istringstream s(aNumber);
    if (!(s >> fileNum)) {
      return -EINVAL;
    }
    return 0;
  }

  std::string getFilename(uint64_t fileNum) {
    static const auto numDir = config.dirPrefix.size();
    return config.dirPrefix[fileNum % numDir] + buildFileName(fileNum);
  }
};

FixedSizeFileManager fileMgr;

using gobjfs::stats::StatsCounter;

struct BenchInfo {
  StatsCounter<uint64_t> readLatency;
  uint64_t failedReads{0};
  uint64_t failedWrites{0};
  std::atomic<uint64_t> iops{0};
};

BenchInfo globalBenchInfo;

class StatusExt;

struct ThreadCtx {

  client_ctx_attr_ptr ctx_attr_ptr;
  client_ctx_ptr ctx_ptr;

  uint32_t minFiles;
  uint32_t maxFiles;
  uint32_t maxBlocks;

  uint64_t perThreadIO{0};

  BenchInfo benchInfo;
};


static void doRandomRead(ThreadCtx *ctx) {

  std::mt19937 seedGen(getpid() + gobjfs::os::GetCpuCore());
  std::uniform_int_distribution<decltype(ctx->maxFiles)> filenumGen(
      0, ctx->maxFiles - 1);
  std::uniform_int_distribution<decltype(ctx->maxBlocks)> blockGenerator(
      0, ctx->maxBlocks - 1);

  ctx->ctx_attr_ptr = ctx_attr_new();
  ctx_attr_set_transport(ctx->ctx_attr_ptr, config.transport, config.ipAddress, config.port);

  ctx->ctx_ptr = ctx_new(ctx->ctx_attr_ptr);
  assert(ctx->ctx_ptr);

  int err = ctx_init(ctx->ctx_ptr);
  assert(err == 0);

  std::vector<giocb *> iocb_vec;

  gobjfs::stats::Timer timer(true);

  uint64_t doneCount = 0;
  uint64_t progressCount = 0;

  while (doneCount < ctx->perThreadIO) {
    char* rbuf = (char*) malloc(config.blockSize);
    assert(rbuf);

    giocb* iocb = (giocb *)malloc(sizeof(giocb));
    iocb->aio_buf = rbuf;
    iocb->aio_offset = blockGenerator(seedGen) * config.blockSize; 
    iocb->aio_nbytes = config.blockSize;

    auto filename = fileMgr.getFilename(filenumGen(seedGen));

    auto ret = aio_readcb(ctx->ctx_ptr, filename.c_str(), iocb, nullptr);

    if (ret != 0) {
      free(iocb);
      free(rbuf);
      ctx->benchInfo.failedReads ++;
    } else {
      iocb_vec.push_back(iocb);
    }

    if (doneCount % config.maxOutstandingIO == 0) {
      for (auto& elem : iocb_vec) {
        aio_suspend(ctx->ctx_ptr, elem, nullptr);
        aio_finish(ctx->ctx_ptr, elem);
        // TODO check and incr failed reads
        free(elem->aio_buf);
        free(elem);
      }
      iocb_vec.clear();
    }

    doneCount ++;
    progressCount ++;
    if (progressCount == ctx->perThreadIO/10) {
      LOG(INFO) << ((doneCount * 100)/ctx->perThreadIO) << " percent of work done";
      progressCount = 0;
    }
  }

  int64_t timeMilli = timer.elapsedMilliseconds();
  std::ostringstream s;

  ctx->benchInfo.iops = (ctx->perThreadIO * 1000 / timeMilli);

  s << "thread=" << gobjfs::os::GetCpuCore() 
    << ":num_io=" << ctx->perThreadIO
    << ":time(msec)=" << timeMilli
    << ":iops=" << ctx->benchInfo.iops 
    << ":failed_reads=" << ctx->benchInfo.failedReads
    << ":read_latency(usec)=" << ctx->benchInfo.readLatency
    << std::endl;

  globalBenchInfo.iops += ctx->benchInfo.iops;

  LOG(INFO) << s.str();
}

static constexpr const char* configFileName = "bench_net_client.conf";

int main(int argc, char *argv[]) {
  // google::InitGoogleLogging(argv[0]); TODO logging
  //
  namespace logging = boost::log;
  logging::core::get()->set_filter(logging::trivial::severity >=
      logging::trivial::info);

  {
    struct stat statbuf;
    int err = stat(configFileName, &statbuf);
    if (err != 0) {
      LOG(ERROR) << "need a config file " << configFileName << " in current dir";
      exit(1);
    }
  }
  config.readConfig(configFileName);

  fileMgr.init(config.maxFiles);

  gobjfs::os::CpuStats startCpuStats;
  startCpuStats.getProcessStats();

  std::vector<ThreadCtx *> ctxVec;

  std::vector<std::future<void>> futVec;

  for (decltype(config.maxThr) thr = 0; thr < config.maxThr; thr++) {
    ThreadCtx *ctx = new ThreadCtx;
    ctx->maxBlocks = config.maxBlocks;
    ctx->perThreadIO = config.perThreadIO;

    ctx->minFiles = 0;
    ctx->maxFiles = config.maxFiles;
    auto f = std::async(std::launch::async, doRandomRead, ctx);

    futVec.emplace_back(std::move(f));
    ctxVec.push_back(ctx);
  }

  for (auto &elem : futVec) {
    elem.wait();
  }

  for (auto elem : ctxVec) {
    globalBenchInfo.readLatency += elem->benchInfo.readLatency;
    globalBenchInfo.failedReads += elem->benchInfo.failedReads;
  }

  gobjfs::os::CpuStats endCpuStats;
  endCpuStats.getProcessStats();

  endCpuStats -= startCpuStats;

  {
    std::ostringstream s;

    s << "num_ioexec=" << 1 // TODO
      << ":num_threads=" << config.maxThr
      << ":iops=" << globalBenchInfo.iops
      << ":read_latency(usec)=" << globalBenchInfo.readLatency
      << ":cpu_perc=" << endCpuStats.getCpuUtilization();

    LOG(INFO) << s.str();

    if (globalBenchInfo.failedReads) {
      LOG(ERROR) << "failed reads=" << globalBenchInfo.failedReads;
    }
  }

  {
    std::ostringstream s;
    // Print in csv format for easier input to excel
    s << config.maxThr << ","
      << globalBenchInfo.iops << "," 
      << globalBenchInfo.readLatency.mean() << "," 
      << endCpuStats.getCpuUtilization();

    std::cout << s.str() << std::endl;
  }
}
