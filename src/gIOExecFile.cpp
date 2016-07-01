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

#include <FilerJob.h>
#include <IOExecutor.h>
#include <Mempool.h>
#include <gIOExecFile.h>
#include <gMempool.h>
#include <gobjfs_log.h>
#include <util/os_utils.h>
#include <gparse.h>

#include <mutex>
#include <fcntl.h>
#include <linux/limits.h> // PATH_MAX
#include <sys/types.h>
#include <unistd.h>
#include <boost/version.hpp>

using gobjfs::IOExecutor;
using gobjfs::FilerJob;
using gobjfs::FileOp;
using gobjfs::os::IsDirectIOAligned;

// objpool holds freelist of batches with only one fragment
// Not used anymore
// static gobjfs::MempoolSPtr objpool =
// gobjfs::MempoolFactory::createObjectMempool(
//"object", sizeof(gIOBatch) + sizeof(gIOExecFragment));

// =======================================================

gIOBatch *gIOBatchAlloc(size_t count) {
  const size_t allocSize =
      sizeof(gIOBatch) + ((count) * sizeof(gIOExecFragment));

  gIOBatch *ptr = nullptr;
  // ptr = (gIOBatch*)objpool->Alloc(allocSize);
  ptr = (gIOBatch *)malloc(allocSize);

  ptr->count = count;

  for (size_t idx = 0; idx < ptr->count; idx++) {
    gIOExecFragment &frag = ptr->array[idx];
    frag.offset = 0;
    frag.size = 0;
    frag.addr = nullptr;
    frag.completionId = 0;
  }

  return ptr;
}

void gIOBatchFree(gIOBatch *ptr) {

  if (!ptr)
    return;

  for (size_t idx = 0; idx < ptr->count; idx++) {
    gIOExecFragment &frag = ptr->array[idx];
    // assert(IsDirectIOAligned(frag.size));
    assert(IsDirectIOAligned(frag.offset));
    assert(frag.completionId != 0);

    gMempool_free(frag.addr);
  }
  // only if ptr fits the objpool allocSize
  // objpool->Free(ptr);
  free(ptr);
}

// =======================================================

gIOStatusBatch *gIOStatusBatchAlloc(size_t count) {
  const size_t allocSize = sizeof(gIOStatusBatch) + (count * sizeof(gIOStatus));
  return (gIOStatusBatch *)malloc(allocSize);
}

void gIOStatusBatchFree(gIOStatusBatch *ptr) { free(ptr); }

// =======================================================

/* internal representation of ServiceHandle */
struct IOExecServiceInt {
  IOExecutor::Config ioConfig;
  std::vector<std::shared_ptr<IOExecutor>> ioexecVec;

  FileTranslatorFunc fileTranslatorFunc;
  std::mutex mutex;
  gobjfs::stats::StatsCounter<int64_t> fileTranslatorStats_;
  gobjfs::stats::Histogram<int64_t> fileTranslatorHist_;

  // could use std::forward
  int callTranslator(const char *old_name, size_t len, char *new_name) {
    gobjfs::stats::Timer timer(true);

    int ret = 0;
    if (fileTranslatorFunc) {
      ret = fileTranslatorFunc(old_name, len, new_name);
    } else {
      strncpy(new_name, old_name, PATH_MAX - 1);
    }

    auto time = timer.elapsedNanoseconds();

    // lock the stats update
    std::unique_lock<std::mutex> l(mutex);
    fileTranslatorStats_ = time;
    fileTranslatorHist_ = time;
    return ret;
  }

  std::string getFileTranslatorStats() const {

    std::ostringstream s;

    s << " fileTranslatorStats=" << fileTranslatorStats_
      << ",fileTranslatorHist=" << fileTranslatorHist_ << std::endl;

    return s.str();
  }

  int32_t getSlot(const char *fileName) {
    static std::hash<std::string> hasher;
    auto slot = hasher(fileName) % ioexecVec.size();
    return slot;
  }

  bool isValid() { return (ioexecVec.size() > 0); }
};

int32_t IOExecGetNumExecutors(IOExecServiceHandle serviceHandle) {
  return serviceHandle->ioexecVec.size();
}

int32_t IOExecGetStats(IOExecServiceHandle serviceHandle, char *buf,
                       int32_t len) {
  decltype(len) curOffset = 0;

  auto str = serviceHandle->getFileTranslatorStats();
  uint32_t copyLen = str.size();
  if ((ssize_t)str.size() >= len - curOffset) {
    // truncate the string to be copied
    copyLen = len;
  }
  strncpy(buf + curOffset, str.c_str(), copyLen);
  curOffset += copyLen;

  if (curOffset < len) {

    for (auto &elem : serviceHandle->ioexecVec) {
      auto str = elem->getState();
      uint32_t copyLen = str.size();
      if ((ssize_t)str.size() >= len - curOffset) {
        // truncate the string to be copied
        copyLen = len;
      }
      strncpy(buf + curOffset, str.c_str(), copyLen);
      curOffset += copyLen;
      if (curOffset >= len) {
        LOG(WARNING) << "input buffer len=" << len << " insufficient for stats";
        break;
      }
    }
  }

  return curOffset;
}

static int32_t doCommonInit(IOExecServiceHandle handle) {
  int32_t ret = 0;

  do {
    if (handle->ioConfig.cpuCores_.size()) {
      for (auto &elem : handle->ioConfig.cpuCores_) {
        const std::string name = "ioexecfile" + std::to_string(elem);
        try {
          auto sptr =
              std::make_shared<IOExecutor>(name, elem, handle->ioConfig);
          handle->ioexecVec.emplace_back(sptr);
        } catch (const std::exception &e) {
          LOG(ERROR) << "failed to alloc IOExecutor for core=" << elem
                     << " exception=" << e.what();
          ret = -ENOMEM;
          break;
        }
      }
    } else {
      ret = -EINVAL;
    }
  } while (0);

  // google::FlushLogFiles(0); TODO logging
  return ret;
}

IOExecServiceHandle IOExecFileServiceInit(int32_t numCoresForIO,
                                          int32_t queueDepthForIO,
                                          FileTranslatorFunc fileTranslatorFunc,
                                          bool createFlag) {

  int ret = 0;

  if ((numCoresForIO == 0) || (queueDepthForIO == 0)) {
    LOG(ERROR) << "parameters need to be non-zero "
               << " numCores=" << numCoresForIO
               << " queueDepth=" << queueDepthForIO;
    return nullptr;
  }

  IOExecServiceHandle handle = new IOExecServiceInt;
  handle->fileTranslatorFunc = fileTranslatorFunc;

  for (int32_t idx = 0; idx < numCoresForIO; idx++) {
    handle->ioConfig.cpuCores_.push_back(idx);
  }
  handle->ioConfig.queueDepth_ = queueDepthForIO;

  ret = doCommonInit(handle);

  if (ret != 0) {
    delete handle;
    handle = nullptr;
  }

  return handle;
}

IOExecServiceHandle IOExecFileServiceInit(const char *pConfigFileName,
                                          FileTranslatorFunc fileTranslatorFunc,
                                          bool createFlag) {

  int32_t ret = 0;

  IOExecServiceHandle handle = new IOExecServiceInt;
  handle->fileTranslatorFunc = fileTranslatorFunc;

  do {
    if (ParseConfigFile(pConfigFileName, handle->ioConfig) < 0) {
      LOG(ERROR) << "Invalid Config File=" << pConfigFileName;
      ret = -EINVAL;
      break;
    }

    if (handle->ioConfig.cpuCores_.size() == 0) {
      LOG(ERROR) << "config file=" << pConfigFileName
                 << " has zero cpuCores allocated."
                 << "  This can happen if your binary is linked to incorrect "
                    "boost version."
                 << "  Is your binary linked to boost_program_options version="
                 << BOOST_LIB_VERSION;
      ret = -EINVAL;
      break;
    }

    ret = doCommonInit(handle);

  } while (0);

  if (ret != 0) {
    delete handle;
    handle = nullptr;
  }

  return handle;
}

int32_t IOExecFileServiceDestroy(IOExecServiceHandle serviceHandle) {
  if (!serviceHandle) {
    LOG(ERROR) << "service handle is invalid";
    return -EINVAL;
  }

  for (auto elem : serviceHandle->ioexecVec) {
    elem->stop();
    elem.reset();
  }
  delete serviceHandle;
  return 0;
}

// =======================================================

/* internal representation of EventFdHandle */
struct IOExecEventFdInt {
  int fd[2]{-1, -1};

  IOExecEventFdInt(int in_fd[]) {
    fd[0] = in_fd[0];
    fd[1] = in_fd[1];
    assert(fd[0] >= 0);
    assert(fd[1] >= 0);
  }

  ~IOExecEventFdInt() {
    close(fd[0]);
    close(fd[1]);
  }
};

IOExecEventFdHandle IOExecEventFdOpen(IOExecServiceHandle serviceHandle) {

  IOExecEventFdHandle eventFdPtr = nullptr;

  if (!serviceHandle || !serviceHandle->isValid()) {
    LOG(ERROR) << "service handle is invalid";
    return eventFdPtr;
  }

  // open pipe
  int fd[2];

  int retcode = pipe(fd);

  if (retcode != 0) {
    LOG(ERROR) << "failed to allocate pipe errno=" << errno;
    close(fd[0]);
    close(fd[1]);
  } else {
    int pipeSz = fcntl(fd[1], F_GETPIPE_SZ);
    if (pipeSz != -1) {
      LOG(INFO) << " created pipes=" << fd[0] << ":" << fd[1]
                << ":size=" << pipeSz;
    } else {
      LOG(WARNING) << " Failed to get pipesz for fd=" << fd[1]
                   << ":errno=" << -errno;
    }
    eventFdPtr = new IOExecEventFdInt(fd);
  }

  return eventFdPtr;
}

int32_t IOExecEventFdClose(IOExecEventFdHandle eventFdPtr) {
  delete eventFdPtr;
  return 0;
}

int IOExecEventFdGetReadFd(IOExecEventFdHandle eventFdPtr) {
  if (!eventFdPtr) {
    LOG(ERROR) << "Rejecting GetReadFd attempt with null eventHandle";
    return gobjfs::os::FD_INVALID;
  }
  return eventFdPtr->fd[0];
}

// =======================================================

/* internal representation of FileHandle */
struct IOExecFileInt {
  IOExecServiceHandle serviceHandle;
  int fd{-1};
  CoreId core{CoreIdInvalid};

  IOExecFileInt(IOExecServiceHandle serviceHandle, int fd, CoreId core)
      : serviceHandle(serviceHandle), fd(fd), core(core) {
    assert(fd >= 0);
  }

  ~IOExecFileInt() {
    if (fd != -1) {
      int ret = close(fd);
      if (ret != 0) {
        LOG(ERROR) << "failed to close fd=" << fd << " errno=" << errno;
      }
    }
  }
};

IOExecFileHandle IOExecFileOpen(IOExecServiceHandle serviceHandle,
                                const char *fileName, size_t fileNameLength,
                                int32_t flags) {

  IOExecFileHandle newHandle{nullptr};

  if (!serviceHandle || !serviceHandle->isValid()) {
    LOG(ERROR) << "service handle is invalid";
    return newHandle;
  }

  char absFileName[PATH_MAX];

  const int translateRet =
      serviceHandle->callTranslator(fileName, fileNameLength, absFileName);

  if (translateRet < 0) {
    LOG(ERROR) << "file translation failed for fileName="
               << std::string(fileName, fileNameLength);
    return nullptr;
  }

  // user must add O_DIRECT for aligned IO
  int newFlags = flags;

  int mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;

  int fd = open(absFileName, newFlags, mode);

  if (fd < 0) {
    int capture_errno = errno;
    LOG(ERROR) << "failed to open file=" << absFileName << " flags=" << newFlags
               << " mode=" << mode << " errno=" << capture_errno;
  } else {
    CoreId core = serviceHandle->getSlot(fileName);
    newHandle = new IOExecFileInt(serviceHandle, fd, core);
  }
  return newHandle;
}

int32_t IOExecFileClose(IOExecFileHandle fileHandle) {
  delete fileHandle;
  return 0;
}

int32_t IOExecFileTruncate(IOExecFileHandle fileHandle, size_t newSize) {

  if (!fileHandle) {
    LOG(ERROR) << "Rejecting truncate with null file handle";
    return -EINVAL;
  }

  int ret = ftruncate(fileHandle->fd, newSize);
  if (ret != 0) {
    ret = -errno;
    LOG(ERROR) << "truncate failed ret=" << errno;
  }

  return ret;
}

static int32_t IOExecFileOp(const char *name, FileOp optype,
                            IOExecFileHandle fileHandle, bool closeFileHandle,
                            const gIOBatch *batch,
                            IOExecEventFdHandle eventFdHandle) {

  int retcode = 0;

  if (!eventFdHandle || (eventFdHandle->fd[1] == gobjfs::os::FD_INVALID)) {
    LOG(ERROR) << "Rejecting " << name << " with invalid eventfd";
    return -EINVAL;
  }

  if (!fileHandle) {
    LOG(ERROR) << "Rejecting " << name << " with invalid file handle";
    return -EINVAL;
  }

  int jobFd = eventFdHandle->fd[1];

  gobjfs::IOExecutorSPtr ioexecPtr;
  try {
    ioexecPtr = fileHandle->serviceHandle->ioexecVec.at(fileHandle->core);
  } catch (const std::exception &e) {
    LOG(ERROR) << "entry=" << fileHandle->core
               << " doesnt exist in ioexec vector of size="
               << fileHandle->serviceHandle->ioexecVec.size();
    return -EINVAL;
  }

  // cache batch->count before the loop
  // because the batch can be freed after submitTask
  // making any read of batch->count incorrect
  const decltype(batch->count) totalCount = batch->count;
  for (decltype(batch->count) idx = 0; idx < totalCount; idx++) {
    const gIOExecFragment &frag = batch->array[idx];
    if ((frag.size == 0) || (frag.addr == nullptr)) {
      continue;
    }
    auto job = new FilerJob(fileHandle->fd, optype);
    job->setBuffer(frag.offset, (char *)frag.addr, frag.size);
    job->completionId_ = frag.completionId;
    job->completionFd_ = jobFd;
    job->canBeFreed_ = true; // free job after completion
    job->closeFileHandle_ = closeFileHandle;
    retcode = ioexecPtr->submitTask(job, /*blocking*/ false);
    if (retcode != 0) {
      LOG(WARNING) << "job not submitted due to overflow";
      delete job; // if not submitted
    }
  }
  return retcode;
}

int32_t IOExecFileWrite(IOExecFileHandle fileHandle, const gIOBatch *batch,
                        IOExecEventFdHandle eventFdHandle) {

  bool closeFileHandle = false;
  return IOExecFileOp("write", FileOp::Write, fileHandle, closeFileHandle,
                      batch, eventFdHandle);
}

int32_t IOExecFileRead(IOExecFileHandle fileHandle, const gIOBatch *batch,
                       IOExecEventFdHandle eventFdHandle) {

  bool closeFileHandle = false;
  return IOExecFileOp("read", FileOp::Read, fileHandle, closeFileHandle, batch,
                      eventFdHandle);
}

int32_t IOExecFileRead(IOExecServiceHandle serviceHandle, const char *fileName,
                       size_t fileNameLength, const gIOBatch *batch,
                       IOExecEventFdHandle eventFdHandle) {

  auto fileHandle =
      IOExecFileOpen(serviceHandle, fileName, fileNameLength, O_RDONLY);
  if (fileHandle == nullptr) {
    return -EIO;
  }

  bool closeFileHandle = true;
  auto ret = IOExecFileOp("read", FileOp::Read, fileHandle, closeFileHandle,
                          batch, eventFdHandle);

  // close fileHandle but do not close fd which is still in use
  // because the fd will be closed after FilerJob::reset
  fileHandle->fd = gobjfs::os::FD_INVALID;
  IOExecFileClose(fileHandle);
  return ret;
}

int32_t IOExecFileDeleteSync(IOExecServiceHandle serviceHandle,
                             const char *fileName) {
  (void)serviceHandle;

  const size_t fileNameLength = strlen(fileName);

  char absFileName[PATH_MAX];
  const int translateRet =
      serviceHandle->callTranslator(fileName, fileNameLength, absFileName);
  if (translateRet < 0) {
    LOG(ERROR) << "file translation failed for fileName="
               << std::string(fileName, fileNameLength);
    return -EINVAL;
  }

  int retcode = ::unlink(absFileName);
  if (retcode != 0) {
    retcode = -errno;
    LOG(ERROR) << "failed to delete file=" << fileName << " errno=" << errno;
  }
  return retcode;
}

int32_t IOExecFileDelete(IOExecServiceHandle serviceHandle,
                         const char *fileName, gCompletionID completionId,
                         IOExecEventFdHandle eventFdHandle) {

  const size_t fileNameLength = strlen(fileName);

  if (!serviceHandle || !serviceHandle->isValid()) {
    LOG(ERROR) << "service handle is invalid";
    return -EINVAL;
  }

  if (!eventFdHandle || (eventFdHandle->fd[1] == gobjfs::os::FD_INVALID)) {
    LOG(ERROR) << "Rejecting delete with invalid eventfd";
    return -EINVAL;
  }

  char absFileName[PATH_MAX];

  const int translateRet =
      serviceHandle->callTranslator(fileName, fileNameLength, absFileName);
  if (translateRet < 0) {
    LOG(ERROR) << "file translation failed for fileName="
               << std::string(fileName, fileNameLength);
    return -EINVAL;
  }

  auto job = new FilerJob(absFileName, FileOp::Delete);
  job->completionId_ = completionId;
  job->completionFd_ = eventFdHandle->fd[1];
  job->canBeFreed_ = true; // free job after completion
  int retcode = serviceHandle->ioexecVec[0]->submitTask(job, true);
  if (retcode != 0) {
    LOG(WARNING) << "delete job not submitted due to overflow";
    delete job; // if not submitted
  }
  return retcode;
}

// ============================================

EXTERNC {
  service_handle_t gobjfs_ioexecfile_service_init(
      const char *cfg_name, FileTranslatorFunc trans_func) {
    return IOExecFileServiceInit(cfg_name, trans_func, true);
  }

  int32_t gobjfs_ioexecfile_service_destroy(service_handle_t service_handle) {
    int32_t rc = IOExecFileServiceDestroy(service_handle);
    return rc;
  }

  handle_t gobjfs_ioexecfile_file_open(service_handle_t service_handle,
                                       const char *name, size_t name_length,
                                       int options) {
    IOExecFileHandle h =
        IOExecFileOpen(service_handle, name, name_length, options);
    return (handle_t)h;
  }

  int32_t gobjfs_ioexecfile_file_write(handle_t handle, const batch_t *batchp,
                                       event_t eventFd) {
    return IOExecFileWrite((IOExecFileHandle)handle, (gIOBatch *)batchp,
                           (IOExecEventFdHandle)eventFd);
  }

  int32_t gobjfs_ioexecfile_file_read(handle_t handle, batch_t * batchp,
                                      event_t eventFd) {
    return IOExecFileRead((IOExecFileHandle)handle, (gIOBatch *)batchp,
                          (IOExecEventFdHandle)eventFd);
  }

  int32_t gobjfs_ioexecfile_file_delete(service_handle_t service_handle,
                                        const char *name, completion_id_t cid,
                                        event_t eventFd) {
    return IOExecFileDelete(service_handle, name, cid,
                            (IOExecEventFdHandle)eventFd);
  }

  int32_t gobjfs_ioexecfile_file_truncate(handle_t handle, size_t new_size) {
    return IOExecFileTruncate(handle, new_size);
  }

  int32_t gobjfs_ioexecfile_file_close(handle_t handle) {
    return IOExecFileClose((IOExecFileHandle)handle);
  }

  event_t gobjfs_ioexecfile_event_fd_open(service_handle_t service_handle) {
    auto h = IOExecEventFdOpen(service_handle);
    return (event_t)h;
  }

  int32_t gobjfs_ioexecfile_event_fd_close(event_t eventFd) {
    return IOExecEventFdClose((IOExecEventFdHandle)eventFd);
  }

  int gobjfs_ioexecfile_event_fd_get_read_fd(event_t eventFd) {
    return IOExecEventFdGetReadFd((IOExecEventFdHandle)eventFd);
  }

  batch_t *gobjfs_batch_alloc(int n) { return gIOBatchAlloc(n); }

  void gobjfs_debug_fragment(const void *p) {
    const gIOExecFragment *fp = (gIOExecFragment *)p;
    LOG(INFO) << p << " :     fragment{ "
              << " completionId = " << fp->completionId
              << "; offset=" << fp->offset << "; size=" << fp->size
              << "; addr=" << (void *)(fp->addr) << " }";
  }

  void gobjfs_debug_batch(const batch_t *p) {
    const gIOExecFragment *fp = p->array;
    LOG(INFO) << p << " : batch { count= " << p->count << " ; "
              << " &array:" << fp << " elements: ";

    gobjfs_debug_fragment(fp);
    LOG(INFO) << "}";
  }

  int32_t gobjfs_ioexecfile_service_getstats(service_handle_t service_handle,
                                             char *buffer, int32_t len) {
    return IOExecGetStats(service_handle, buffer, len);
  }
}
