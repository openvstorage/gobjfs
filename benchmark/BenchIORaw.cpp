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

#include <Mempool.h>
#include <gIOExecFile.h>
#include <gMempool.h>
#include <util/CpuStats.h>
#include <util/ShutdownNotifier.h>
#include <util/Stats.h>
#include <util/Timer.h>
#include <util/os_utils.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <boost/program_options.hpp>
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
#include <future>

using namespace boost::program_options;

struct Config {
  uint32_t blockSize = 16384;
  uint32_t alignSize = 4096;
  uint32_t maxBlocks = 10000;

  uint64_t perThreadIO = 10000;
  bool doMemCheck = false;
  uint32_t maxThr = 1;
  uint32_t writePercent = 0;
  uint32_t totalReadWriteScale = 2000;
  std::string dirPrefix;

  int readConfig(const std::string &configFileName) {
    options_description desc("allowed options");
    desc.add_options()
      ("block_size", value<uint32_t>(&blockSize)->required(), "blocksize for reads & writes")
      ("max_file_blocks", value<uint32_t>(&maxBlocks)->required(), "number of [blocksize] blocks in file")
      ( "align_size", value<uint32_t>(&alignSize)->required(), "size that memory is aligned to ")
      ( "per_thread_max_io", value<uint64_t>(&perThreadIO)->required(), "number of ops to execute")
      ("do_mem_check", value<bool>(&doMemCheck)->required(), "compare read buffer")
      ( "max_threads", value<uint32_t>(&maxThr)->required(), "max threads")
      ( "write_percent", value<uint32_t>(&writePercent)->required(), "percent of total read write scale")
      ( "total_read_write_scale", value<uint32_t>(&totalReadWriteScale)->required(), "total scale")
      ("mountpoint", value<std::string>(&dirPrefix)->required()->multitoken(), "ssd mount point");

    std::ifstream configFile(configFileName);
    variables_map vm;
    store(parse_config_file(configFile, desc), vm);
    notify(vm);

    LOG(INFO)
        << "================================================================="
        << std::endl << "     BenchIOExecFile config" << std::endl
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
      else if (auto v = boost::any_cast<bool>(&value))
        s << *v << std::endl;
      else if (auto v = boost::any_cast<std::vector<std::string>>(&value)) {
        for (auto dirName : *v) {
          s << dirName << ",";
        }
        s << std::endl;
      } else
        s << "cannot interpret value" << std::endl;
    }
    LOG(INFO) << s.str();
    LOG(INFO)
        << "=================================================================="
        << std::endl;

    return 0;
  }
};

static Config config;

#define FOURMB (4 << 20)

using gobjfs::stats::StatsCounter;

StatsCounter<uint64_t> totalReadLatency;
StatsCounter<uint64_t> totalWriteLatency;
StatsCounter<uint64_t> totalDeleteLatency;
std::atomic<uint64_t> totalIOPs{0};

class StatusExt;

struct ThreadCtx {
  IOExecServiceHandle serviceHandle;
  uint32_t maxBlocks;

  uint64_t perThreadIO{0};

  StatsCounter<uint64_t> totalWriteLatency;
  StatsCounter<uint64_t> totalReadLatency;
  StatsCounter<uint64_t> totalDeleteLatency;

  gobjfs::os::ShutdownNotifier ioCompletionThreadShutdown;

  int writePercent{0}; // between 0 -100
};

static gobjfs::MempoolSPtr objpool;

struct StatusExt {
  enum OpType { Invalid, Read, Write, Freed };

  static void *operator new(size_t sz) { return objpool->Alloc(sz); }

  static void operator delete(void *ptr) { objpool->Free(ptr); }

  bool isRead() const { return (op == Read); }
  bool isWrite() const { return (op == Write); }

  StatusExt() {
    op = Invalid;
    tid = gettid();
  }

  ~StatusExt() {
    op = Freed;
    tid = gettid();
  }

  uint32_t tid{0};
  gIOBatch *batch{nullptr};
  gobjfs::stats::Timer timer;
  IOExecFileHandle handle{nullptr};
  OpType op{Invalid};
};

static int wait_for_iocompletion(int epollfd, int efd, ThreadCtx *ctx) {
  LOG(INFO) << "polling " << efd << " for " << ctx->perThreadIO;

#define MAX_EVENTS 10
  epoll_event events[MAX_EVENTS];

  uint64_t ctr = 0;

  bool terminate = false;

  while (1) {
    if (terminate && (ctr == ctx->perThreadIO)) {
      break;
    }

    int n = epoll_wait(epollfd, events, MAX_EVENTS, -1);

    if (n < 0) {
      if (errno != EINTR) {
        LOG(ERROR) << "epoll failed errno=" << errno;
      }
      continue;
    }

    for (int i = 0; i < n; i++) {
      epoll_event &thisEvent = events[i];

      if (efd == thisEvent.data.fd) {
        gIOStatus iostatus;
        ssize_t ret = read(efd, &iostatus, sizeof(iostatus));
        if (ret == sizeof(iostatus)) {

          VLOG(1) << "Received event (completionId: " << iostatus.completionId
                  << " , status: " << iostatus.errorCode;

          {
            StatusExt *ext =
                reinterpret_cast<StatusExt *>(iostatus.completionId);
            if (ext->isWrite()) {
              ctx->totalWriteLatency = ext->timer.elapsedMicroseconds();
              ctr++;
            } else if (ext->isRead()) {
              ctx->totalReadLatency = ext->timer.elapsedMicroseconds();
              ctr++;
            } else {
              LOG(FATAL) << "unknown opcode";
            }

            assert(ext->batch->count == 1);
            assert(ext->batch->array[0].completionId == iostatus.completionId);

            gIOBatchFree(ext->batch);
            delete ext;
          }
        } else {
          LOG(ERROR) << "incomplete read retcode=" << ret;
          assert(false);
        }
      } else if (thisEvent.data.ptr == &ctx->ioCompletionThreadShutdown) {
        LOG(INFO) << "got shutdown request ";
        uint64_t ctr;
        ctx->ioCompletionThreadShutdown.recv(ctr);
        terminate = true;
      }
    }

    struct epoll_event event;
    event.data.fd = efd;
    event.events = EPOLLIN | EPOLLONESHOT;
    int s = epoll_ctl(epollfd, EPOLL_CTL_MOD, efd, &event);
    assert((s == 0) || (errno == EEXIST));
    (void)s;
  }

  if (ctr != ctx->perThreadIO)
    LOG(ERROR) << "received " << ctr << " events";

  google::FlushLogFiles(0);
  return 0;
}

static void doRandomReadWrite(ThreadCtx *ctx) {
  std::mt19937 seedGen(getpid() + gobjfs::os::GetCpuCore());
  std::uniform_int_distribution<decltype(ctx->maxBlocks)> blockGenerator(
      0, ctx->maxBlocks - 1);
  std::uniform_int_distribution<uint32_t> readWriteRatio(
      0, config.totalReadWriteScale);

  int epollfd = epoll_create1(0);
  assert(epollfd >= 0);

  IOExecEventFdHandle evHandle = IOExecEventFdOpen(ctx->serviceHandle);
  if (evHandle == nullptr) {
    LOG(ERROR) << "eventfd open failed";
    return;
  }

  int efd = IOExecEventFdGetReadFd(evHandle);

  {
    epoll_event event;
    event.data.fd = efd;
    event.events = EPOLLIN | EPOLLONESHOT;
    int s = epoll_ctl(epollfd, EPOLL_CTL_ADD, efd, &event);
    int capture_errno = errno;
    // if epoll already had the fd, it emits error EEXIST
    assert(s == 0 || capture_errno == EEXIST);
    (void)capture_errno;
  }

  ctx->ioCompletionThreadShutdown.init(epollfd);

  std::thread ioCompletionThread(wait_for_iocompletion, epollfd, efd, ctx);

  gobjfs::stats::Timer timer(true);

  bool ReadsAreEnabled = true;
  bool StartCounting = true;

  if ((ctx->writePercent & 0x01) != 0) {
    // so deletes and creates evenly balanced
    ctx->writePercent += 1;
  }

  uint32_t readThreshold = ctx->writePercent;

  uint64_t totalIO = 0;

  IOExecFileHandle handle = 
        IOExecFileOpen(ctx->serviceHandle, 
            config.dirPrefix.c_str(), 
            O_RDWR | O_SYNC | O_CREAT);

  while (totalIO < ctx->perThreadIO) {

    int ret = 0;

    StatusExt *ext = new StatusExt;

    ext->timer.reset();

    auto num = readWriteRatio(seedGen);

    if (ReadsAreEnabled && (num >= readThreshold)) {
      // its a read
      ext->op = StatusExt::Read;
      ext->handle = handle;
    } else {
      // its a create
      ext->op = StatusExt::Write;
      ext->handle = handle;
    }

    ext->batch = gIOBatchAlloc(1);

    {
      gIOExecFragment &frag = ext->batch->array[0];

      frag.completionId = reinterpret_cast<uint64_t>(ext);

      uint64_t blockNum = blockGenerator(seedGen);
      if (ext->isWrite()) {
        frag.offset = blockNum * config.blockSize;
        frag.size = config.blockSize * config.maxBlocks;
        frag.addr = (caddr_t)gMempool_alloc(frag.size);
        memset(frag.addr, 'a' + (blockNum % 26), frag.size);
      } else if (ext->isRead()) {
        frag.offset = blockNum * config.blockSize;
        frag.size = config.blockSize;
        frag.addr = (caddr_t)gMempool_alloc(frag.size);
      } else {
        LOG(FATAL) << "unknown op";
      }

      assert(frag.addr != nullptr);
    }

    do {
      assert(ext->batch->count == 1);
      if (ext->isWrite()) {
        ret = IOExecFileWrite(handle, ext->batch, evHandle);
      } else {
        ret = IOExecFileRead(handle, ext->batch, evHandle);
      }
      if (ret == -EAGAIN) {
        usleep(1);
        LOG(WARNING) << "too fast";
      } else {
        if (StartCounting) {
          totalIO++;
          if (totalIO && totalIO % 10000 == 0) {
            LOG(INFO) << "finished ops=" << totalIO
                      << ":read_latency(usec)=" << ctx->totalReadLatency
                      << ":write_latency(usec)=" << ctx->totalWriteLatency
                      << ":delete_latency(usec)=" << ctx->totalDeleteLatency;
          }
        }
      }
    } while (ret == -EAGAIN);
  }

  ctx->ioCompletionThreadShutdown.send();
  ioCompletionThread.join();

  int64_t timeMilli = timer.elapsedMilliseconds();
  std::ostringstream s;

  s << "thread=" << gobjfs::os::GetCpuCore() << ":num_io=" << ctx->perThreadIO
    << ":time(msec)=" << timeMilli
    << ":read_latency(usec)=" << ctx->totalReadLatency
    << ":write_latency(usec)=" << ctx->totalWriteLatency
    << ":delete_latency(usec)=" << ctx->totalDeleteLatency
    << ":iops=" << ctx->perThreadIO * 1000 / timeMilli << std::endl;

  totalIOPs += (ctx->perThreadIO * 1000 / timeMilli);

  IOExecEventFdClose(evHandle);
  // std::cout << s.str();
  LOG(INFO) << s.str();

  close(epollfd);
}

int main(int argc, char *argv[]) {
  google::InitGoogleLogging(argv[0]);

  config.readConfig("./rawioexec.conf");

  auto serviceHandle = IOExecFileServiceInit("./gioexecfile.conf");

  gMempool_init(config.alignSize);

  objpool =
      gobjfs::MempoolFactory::createObjectMempool("object", sizeof(StatusExt));

  gobjfs::os::CpuStats startCpuStats;
  startCpuStats.getProcessStats();

  std::vector<ThreadCtx *> ctxVec;

  std::vector<std::future<void>> futVec;

  for (decltype(config.maxThr) thr = 0; thr < config.maxThr; thr++) {
    ThreadCtx *ctx = new ThreadCtx;
    ctx->serviceHandle = serviceHandle;
    ctx->maxBlocks = config.maxBlocks;
    ctx->perThreadIO = config.perThreadIO;
    ctx->writePercent = config.writePercent;

    auto f = std::async(std::launch::async, doRandomReadWrite, ctx);

    futVec.emplace_back(std::move(f));
    ctxVec.push_back(ctx);
  }

  for (auto &elem : futVec) {
    elem.wait();
  }

  for (auto elem : ctxVec) {
    totalWriteLatency += elem->totalWriteLatency;
    totalReadLatency += elem->totalReadLatency;
    totalDeleteLatency += elem->totalDeleteLatency;
  }

  gobjfs::os::CpuStats endCpuStats;
  endCpuStats.getProcessStats();

  endCpuStats -= startCpuStats;

  {
    std::ostringstream s;

    s << "num_ioexec=" << IOExecGetNumExecutors(serviceHandle)
      << ":num_threads=" << config.maxThr
      << ":write_perc=" << config.writePercent
      << ":write_scale=" << config.totalReadWriteScale << ":iops=" << totalIOPs
      << ":write_latency(usec)=" << totalWriteLatency
      << ":read_latency(usec)=" << totalReadLatency
      << ":delete_latency(usec)=" << totalDeleteLatency
      << ":cpu_perc=" << endCpuStats.getCpuUtilization();

    LOG(INFO) << s.str();
  }

  {
    std::ostringstream s;
    // Print in csv format for easier input to excel
    s << config.writePercent << "," << config.totalReadWriteScale << ","
      << IOExecGetNumExecutors(serviceHandle) << "," << config.maxThr << ","
      << totalIOPs << "," << totalWriteLatency.mean() << ","
      << totalReadLatency.mean() << "," << totalDeleteLatency.mean() << ","
      << endCpuStats.getCpuUtilization();

    std::cout << s.str() << std::endl;
  }
  IOExecFileServiceDestroy(serviceHandle);
}
