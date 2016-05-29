#pragma once

#include <Queueable.h>
#include <gcommon.h>
#include <libaio.h>
#include <sstream>
#include <string>
#include <util/os_utils.h>

namespace gobjfs {

class IOExecutor;

enum class FileOp : int32_t {
  Nop = 0,
  Write = 1,
  Read = 2,
  Sync = 3,
  Delete = 4,
  // STOP - ENSURE u add to the ostream operator
  // when you change this
};

std::ostream &operator<<(std::ostream &os, const FileOp op);

/**
 * I/Os are issued as FilerJob. FilerJob contains device
 * to which I/O to be issued.
 * classes which are elements of boost lockfree queue
 * cannot have any nontrivial c++ classes as data members
 * therefore, this class is stored by ptr in queue
 */
class FilerJob : public Queueable {
public:
  FileOp op_{FileOp::Nop};
  off_t offset_{0};
  size_t size_{0};

  // returned from kernel
  // default is set to value never returned from IO subsystem
  int retcode_{-EDOM};

  char *buffer_{nullptr};

  // can ioexecutor free this job after execution ?
  bool canBeFreed_{false};

  IOExecutor *executor_{nullptr}; // who executed it

  // Device/File Fd
  int fd_{gobjfs::os::FD_INVALID};
  // fileName set only in case of delete file
  std::string fileName_;
  // Fd used to Notify on completion to application
  int completionFd_{gobjfs::os::FD_INVALID};
  // ID points to I/O
  void *private_{nullptr};
  gCompletionID completionId_{0};
  gContainerID containerId_{0};
  gSegmentID segmentId_{0};

public:
  FilerJob(const char *filename, FileOp op);

  FilerJob(const int fd, FileOp op);

  ~FilerJob();

  GOBJFS_DISALLOW_COPY(FilerJob);
  GOBJFS_DISALLOW_MOVE(FilerJob);

  // check if job params are safe for async io
  bool isValid(std::ostringstream &ostr);

  int32_t prepareCallblock(iocb *cb);

  void setBuffer(off_t fileOffset, char *buffer, size_t size);
  void reset();
};
}
