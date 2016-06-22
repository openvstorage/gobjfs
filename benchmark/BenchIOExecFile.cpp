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

  uint32_t maxFiles = 1000;
  uint32_t maxBlocks = 10000;
  uint64_t perThreadIO = 10000;
  bool doMemCheck = false;
  uint32_t maxThr = 1;
  uint32_t writePercent = 0;
  uint32_t totalReadWriteScale = 2000;
  uint32_t shortenFileSize = 0;
  bool newInstance = false;
  std::vector<std::string> dirPrefix;

  int readConfig(const std::string &configFileName) {
    options_description desc("allowed options");
    desc.add_options()("block_size", value<uint32_t>(&blockSize)->required(),
                       "blocksize for reads & writes")(
        "align_size", value<uint32_t>(&alignSize)->required(),
        "size that memory is aligned to ")(
        "num_files", value<uint32_t>(&maxFiles)->required(), "number of files")(
        "max_file_blocks", value<uint32_t>(&maxBlocks)->required(),
        "number of [blocksize] blocks in file")(
        "per_thread_max_io", value<uint64_t>(&perThreadIO)->required(),
        "number of ops to execute")("do_mem_check",
                                    value<bool>(&doMemCheck)->required(),
                                    "compare read buffer")(
        "max_threads", value<uint32_t>(&maxThr)->required(), "max threads")(
        "write_percent", value<uint32_t>(&writePercent)->required(),
        "percent of total read write scale")(
        "total_read_write_scale",
        value<uint32_t>(&totalReadWriteScale)->required(),
        "total scale")(
        "shorten_file_size", value<uint32_t>(&shortenFileSize), "shorten the"
" size by this much to test nonaliged reads")(
        "new_instance", value<bool>(&newInstance)->required(),
                       "create files from scratch")(
        "mountpoint",
        value<std::vector<std::string>>(&dirPrefix)->required()->multitoken(),
        "ssd mount point");

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

static constexpr size_t FOURMB = (1 << 24);

struct FixedSizeFileManager {
  IOExecServiceHandle serviceHandle;
  std::mutex mutex;
  std::vector<std::string> Directory;

  std::atomic<uint64_t> FilesCtr{0};

  uint32_t createCount{0};
  uint32_t deleteCount{0};

  uint32_t FileSize = FOURMB;
  uint64_t maxFiles_ = 300000;

  void init(IOExecServiceHandle serviceHandleIn, uint32_t MaxFiles,
            bool newInstance) {
    serviceHandle = serviceHandleIn;
    maxFiles_ = MaxFiles;

    if (!newInstance) {

      // read the directory into the Directory vector
      gobjfs::stats::MaxValue<decltype(maxFiles_)> maxCtr;

      for (auto &dirName : config.dirPrefix) {
        DIR *dirp = opendir(dirName.c_str());

        dirent *entryp;
        {
          ssize_t name_max = pathconf(dirName.c_str(), _PC_NAME_MAX);
          if (name_max == -1) /* Limit not defined, or error */
            name_max = 255;   /* Take a guess */
          size_t len = offsetof(struct dirent, d_name) + name_max + 1;
          entryp = (dirent *)malloc(len);
        }

        dirent *result;

        std::string dirString = dirName + "/";
        int ret = 0;
        decltype(maxFiles_) max;
        do {
          ret = readdir_r(dirp, entryp, &result);
          if ((ret == 0) && (result != nullptr)
#ifdef _DIRENT_HAVE_D_TYPE
//&& (entryp->d_type == DT_REG)// doesnt work for xfs ?
#endif
              && (entryp->d_name[0] != '.')) {
            Directory.push_back(dirString + entryp->d_name);
            auto ret = parseFileName(entryp->d_name, max);
            if (ret != 0) {
              LOG(FATAL) << "parse failed";
            }
            maxCtr = max;
          }
        } while ((ret == 0) && (result != nullptr));

        free(entryp);

        ret = closedir(dirp);
        assert(ret == 0);
      }
      FilesCtr = maxCtr.get();
    }
  }

  float spaceUsed() {
    // without mutex
    return ((float)Directory.size() * 100) / maxFiles_;
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
    //return buildFileName(fileNum);
  }

  int createFile(IOExecFileHandle &handle, uint64_t &retFilenum) {
    int ret = 0;

    if (Directory.size() > maxFiles_) {
      return -ENOSPC;
    }

    retFilenum = FilesCtr++;
    auto str = getFilename(retFilenum);

    handle = IOExecFileOpen(serviceHandle, 
        str.c_str(), 
        str.size(),
        O_RDWR | O_SYNC | O_CREAT);

    if (handle != nullptr) {
      std::unique_lock<std::mutex> lck(mutex);
      Directory.push_back(str);
      createCount++;
    }

    return ret;
  }

  IOExecFileHandle openFile(uint32_t index, uint64_t *actualNumber) {
    IOExecFileHandle handle = nullptr;

    const size_t dirSize = Directory.size();
    if (!dirSize)
      return nullptr;

    while (index >= dirSize) {
      index = index >> 1;
    }

    try {
      auto str = Directory.at(index);
      handle = IOExecFileOpen(serviceHandle, 
        str.c_str(), 
        str.size(),
        O_RDONLY);

      if (config.doMemCheck && actualNumber) {
        auto filename = basename(str.c_str());
        auto ret = parseFileName(filename, *actualNumber);
        if (ret != 0) {
          LOG(FATAL) << "parse failed";
        }
      }
    } catch (std::exception &e) {
      LOG(ERROR) << "bad index " << index << " dirsize=" << dirSize;
    }
    return handle;
  }

  int getFileToDelete(uint32_t index, std::string &fname) {
    int ret = 0;

    const size_t dirSize = Directory.size();
    if (!dirSize)
      return -ENOENT;

    {
      std::unique_lock<std::mutex> lck(mutex);

      while (index >= dirSize) {
        index = index >> 1;
      }

      assert(index < Directory.size());

      fname = Directory[index];
      if (index != Directory.size() - 1) {
        // fastest way to erase elem from vector is
        // to swap with last elem
        Directory[index] = std::move(Directory.back());
      }
      Directory.pop_back();
    }

    return ret;
  }
};

FixedSizeFileManager fileMgr;

using gobjfs::stats::StatsCounter;

StatsCounter<uint64_t> totalReadLatency;
StatsCounter<uint64_t> totalWriteLatency;
StatsCounter<uint64_t> totalDeleteLatency;
uint64_t totalFailedReads{0};
uint64_t totalFailedWrites{0};
std::atomic<uint64_t> totalIOPs{0};

class StatusExt;

struct ThreadCtx {
  IOExecServiceHandle serviceHandle;
  uint32_t minFiles;
  uint32_t maxFiles;
  uint32_t maxBlocks;

  uint64_t perThreadIO{0};

  StatsCounter<uint64_t> totalWriteLatency;
  StatsCounter<uint64_t> totalReadLatency;
  StatsCounter<uint64_t> totalDeleteLatency;
  uint64_t failedReads{0};
  uint64_t failedWrites{0};

  gobjfs::os::ShutdownNotifier ioCompletionThreadShutdown;

  int writePercent{0}; // between 0 -100
};

static gobjfs::MempoolSPtr objpool;

struct StatusExt {
  enum OpType { Invalid, Read, Write, Delete, Freed };

  static void *operator new(size_t sz) { return objpool->Alloc(sz); }

  static void operator delete(void *ptr) { objpool->Free(ptr); }

  bool isRead() const { return (op == Read); }
  bool isWrite() const { return (op == Write); }
  bool isDelete() const { return (op == Delete); }

  StatusExt() {
    op = Invalid;
    tid = gettid();
  }

  ~StatusExt() {
    handle = nullptr;
    op = Freed;
    tid = gettid();
  }

  uint32_t dirIndex{0};
  uint64_t actualFilenum{0};
  uint32_t tid{0};   // for debugging
  std::string fname; // for delete
  gIOBatch *batch{nullptr};
  IOExecFileHandle handle{nullptr};
  gobjfs::stats::Timer timer;
  OpType op{Invalid};

};

static constexpr size_t MAX_EVENTS = 10;

static int wait_for_iocompletion(int epollfd, int efd, ThreadCtx *ctx) {
  LOG(INFO) << "polling " << efd << " for " << ctx->perThreadIO;

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
              //IOExecFileTruncate(ext->handle, ext->batch->array[0].size - config.shortenFileSize); NonAligned Option2 truncate after write
              if (iostatus.errorCode != 0) {
                ctx->failedWrites ++;        
              }
            } else if (ext->isRead()) {
              if (config.doMemCheck) {
                gIOExecFragment &frag = ext->batch->array[0];
                char *buf = frag.addr;
                const char expChar = 'a' + (ext->actualFilenum % 26);
                bool failed = false;
                uint32_t bufOffset = 0;
                for (uint32_t bufOffset = 0; bufOffset < frag.size;
                     bufOffset++) {
                  if (buf[bufOffset] != expChar) {
                    failed = true;
                    break;
                  }
                }
                if (failed) {
                  LOG(ERROR) << "Comparison failed at file=" << ext->dirIndex
                             << " offset=" << bufOffset
                             << " expchar=" << expChar
                             << " actual=" << buf[bufOffset];
                }
              }
              if (iostatus.errorCode != 0) {
                ctx->failedReads ++;
              }
              ctx->totalReadLatency = ext->timer.elapsedMicroseconds();
              ctr++;
            } else if (ext->isDelete()) {
              ctx->totalDeleteLatency = ext->timer.elapsedMicroseconds();
              ctr++;
              fileMgr.deleteCount++;
            } else {
              LOG(FATAL) << "unknown opcode";
            }

            if (!ext->isDelete()) {
              IOExecFileClose(ext->handle);
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
  std::uniform_int_distribution<decltype(ctx->maxFiles)> filenumGen(
      0, ctx->maxFiles - 1);
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

  /**
   * if newInstance, change ratio of operations
   * once files get created
   * fileMgr.spaceUsed() < 95, delete:create:read = 0:100:0
   * fileMgr.spaceUsed() > 95, delete:create:read = 5:5:90
   */

  bool ReadsAreEnabled = true;
  bool StartCounting = true;

  bool DeletesAreEnabled = true;

  if ((ctx->writePercent & 0x01) != 0) {
    // so deletes and creates evenly balanced
    ctx->writePercent += 1;
  }
  uint32_t deleteThreshold = ctx->writePercent / 2;
  uint32_t readThreshold = ctx->writePercent;
  decltype(fileMgr.createCount) lastCount = 0;

  if (config.newInstance) {
    deleteThreshold = 100;
    readThreshold = 100;
    DeletesAreEnabled = false;
    ReadsAreEnabled = false;
    StartCounting = false;
  }

  uint64_t totalIO = 0;

  uint32_t sleepTimeMicrosec = 1;

  while (totalIO < ctx->perThreadIO) {
    IOExecFileHandle handle{nullptr};

    int ret = 0;

    StatusExt *ext = new StatusExt;

    ext->timer.reset();

    auto num = readWriteRatio(seedGen);

    if (ReadsAreEnabled && (num >= readThreshold)) {
      // its a read
      ext->dirIndex = filenumGen(seedGen);
      handle = fileMgr.openFile(ext->dirIndex, &ext->actualFilenum);
      if (handle) {
        ext->op = StatusExt::Read;
        ext->handle = handle;
      } else {
        delete ext;
        continue;
      }
    } else if (DeletesAreEnabled && (num >= deleteThreshold)) {
      // its a delete
      ext->dirIndex = filenumGen(seedGen);
      ret = fileMgr.getFileToDelete(ext->dirIndex, ext->fname);
      if (ret == 0) {
        ext->op = StatusExt::Delete;
        // TODO
      } else {
        delete ext;
        continue;
      }
    } else {
      // its a create
      ret = fileMgr.createFile(handle, ext->actualFilenum);
      if (ret != 0) {
        delete ext;
        continue;
      }
      ext->op = StatusExt::Write;
      ext->handle = handle;

      if (fileMgr.createCount && fileMgr.createCount % 10000 == 0) {
        LOG(INFO) << "finished creates=" << fileMgr.createCount
                  << ":latency=" << ctx->totalWriteLatency;
      }

      // recalc threshold after every 20 percent writes
      if (config.newInstance &&
          (!ReadsAreEnabled || !DeletesAreEnabled || !StartCounting) &&
          ((fileMgr.createCount - lastCount) >
           ((20 * fileMgr.maxFiles_) / 100))) {
        auto spaceUsed = fileMgr.spaceUsed();
        lastCount = fileMgr.createCount;

        LOG(INFO) << "recalculating thresholds at " << fileMgr.createCount
                  << " spaceUsed=" << fileMgr.spaceUsed();

        if (!StartCounting && (spaceUsed > 95.0f)) {
          LOG(INFO) << "turning on reads and deletes " << fileMgr.spaceUsed()
                    << " till now write latency=" << ctx->totalWriteLatency;
          ReadsAreEnabled = true;
          DeletesAreEnabled = true;
          readThreshold = ctx->writePercent;
          deleteThreshold = ctx->writePercent / 2;
          StartCounting = true;
        }
      }
    }

    ext->batch = gIOBatchAlloc(1);

    {
      gIOExecFragment &frag = ext->batch->array[0];

      frag.completionId = reinterpret_cast<uint64_t>(ext);

      if (ext->isWrite()) {
        frag.offset = 0;
        frag.size = (config.blockSize * config.maxBlocks) - config.shortenFileSize; // TEST unaligned writes
        frag.addr = (caddr_t)gMempool_alloc(frag.size);
        memset(frag.addr, 'a' + (ext->actualFilenum % 26), frag.size);
        assert(frag.addr != nullptr);
      } else if (ext->isRead()) {
        uint64_t blockNum = blockGenerator(seedGen);
        frag.offset = blockNum * config.blockSize;
        frag.size = config.blockSize - config.shortenFileSize; // TEST unaligned reads
        frag.addr = (caddr_t)gMempool_alloc(frag.size);
        assert(frag.addr != nullptr);
      } else if (ext->isDelete()) {
        // do nothing
      } else {
        LOG(FATAL) << "unknown op";
      }
    }

    do {

      assert(ext->batch->count == 1);

      if (ext->isWrite()) {
        ret = IOExecFileWrite(handle, ext->batch, evHandle);
      } else if (ext->isRead()) {
        ret = IOExecFileRead(handle, ext->batch, evHandle);
      } else if (ext->isDelete()) {
        ret = IOExecFileDelete(ctx->serviceHandle, ext->fname.c_str(),
                               ext->batch->array[0].completionId, evHandle);
      } else {
        LOG(FATAL) << " Unknown opcode";
      }

      if (ret == -EAGAIN) {
	if (sleepTimeMicrosec < 10) 
          sleepTimeMicrosec *= 2;
        usleep(sleepTimeMicrosec);
        LOG(WARNING) << "too fast sleep=" << sleepTimeMicrosec;
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

  s << "thread=" << gobjfs::os::GetCpuCore() 
    << ":num_io=" << ctx->perThreadIO
    << ":failed_reads=" << ctx->failedReads
    << ":failed_writes=" << ctx->failedWrites
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

  config.readConfig("./benchioexec.conf");

  FileTranslatorFunc fileTranslatorFunc {nullptr};

  auto serviceHandle = IOExecFileServiceInit("./gioexecfile.conf", 
    fileTranslatorFunc,
    config.newInstance);

  gMempool_init(config.alignSize);

  objpool =
      gobjfs::MempoolFactory::createObjectMempool("object", sizeof(StatusExt));

  fileMgr.init(serviceHandle, config.maxFiles, config.newInstance);

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

    ctx->minFiles = 0;
    ctx->maxFiles = config.maxFiles;
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
    totalFailedReads += elem->failedReads;
    totalFailedWrites += elem->failedWrites;
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

    if (totalFailedReads || totalFailedWrites) {
      LOG(ERROR) 
        << "failed reads=" << totalFailedReads
        << " failed writes=" << totalFailedWrites
        ;
    }
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
