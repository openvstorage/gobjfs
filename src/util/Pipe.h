#pragma once

#include <unistd.h>
#include <fcntl.h>
#include <memory> // shared_ptr, unique_ptr
#include <gobjfs_log.h> 

class Pipe
{
  int fd_[2];

  public:

  Pipe()
  {
    int ret = pipe2(fd_, O_NONBLOCK | O_CLOEXEC);
    if (ret != 0) {
      GLOG_ERROR("failed to create pipe errno=" << errno);
      throw std::runtime_error("failed to create pipe");
    }
  }

  int getReadFD() {
    return fd_[0];
  }

  int getWriteFD() {
    return fd_[1];
  }

  ~Pipe()
  {
    int ret = close(fd_[1]);
    ret = close(fd_[0]);
    if (ret != 0) {
      GLOG_ERROR("failed to close pipes=" << static_cast<int32_t>(fd_[0]) 
          << "," << static_cast<int32_t>(fd_[1]));
    }
  }
};

typedef std::shared_ptr<Pipe> PipeSPtr;
typedef std::unique_ptr<Pipe> PipeUPtr;
