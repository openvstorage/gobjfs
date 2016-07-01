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

#include "TimerNotifier.h"

#include <cassert>       // assert
#include <gobjfs_log.h>  // LOG
#include <strings.h>     // bzero
#include <sys/epoll.h>   // epoll
#include <sys/timerfd.h> // timerfd
#include <unistd.h>      // read

namespace gobjfs {
namespace os {

int32_t TimerNotifier::init(int epollFD, int timeoutSec, int timeoutNanosec) {
  int ret = 0;

  fd_ = timerfd_create(CLOCK_MONOTONIC, 0);
  if (fd_ < 0) {
    fd_ = -1;
    ret = -errno;
    LOG(ERROR) << "Failed to create timerfd errno=" << ret;
    return ret;
  }

  struct itimerspec new_value;
  new_value.it_value.tv_sec = 60;
  new_value.it_value.tv_nsec = 0;
  new_value.it_interval.tv_sec = timeoutSec;
  new_value.it_interval.tv_nsec = timeoutNanosec;

  ret = timerfd_settime(fd_, 0, &new_value, NULL);
  if (ret < 0) {
    ret = -errno;
    LOG(ERROR) << "Failed to set time timerfd errno=" << ret;
    return ret;
  }

  epoll_event epollEvent;
  bzero(&epollEvent, sizeof(epollEvent));
  epollEvent.data.ptr = this;

  epollEvent.events = EPOLLIN | EPOLLPRI | EPOLLERR | EPOLLHUP;
  ret = epoll_ctl(epollFD, EPOLL_CTL_ADD, fd_, &epollEvent);
  if (ret < 0) {
    ret = -errno;
    LOG(ERROR) << "Failed to add fd=" << fd_ << " to epollfd=" << epollFD
               << " errno=" << ret;
  } else {
    LOG(INFO) << "epollfd=" << epollFD << " registered timer fd=" << fd_;
  }
  return ret;
}

int32_t TimerNotifier::recv() {
  int ret = 0;
  uint64_t count = 0;

  ssize_t readSize = read(fd_, &count, sizeof(count));

  if (readSize != sizeof(count)) {

    count = 0;

    if (readSize == -1)
      ret = -errno;
    else
      ret = -EINVAL;

    LOG(ERROR) << "failed to read fd=" << fd_ << " readSize=" << readSize
               << " expectedSize=" << sizeof(count) << " errno=" << ret;
  }
  return count;
}

int32_t TimerNotifier::destroy() {
  /* epoll fd itself will be closed
    int retcode = epoll_ctl(epollFD_, EPOLL_CTL_DEL, fd_, NULL);
    if (retcode != 0)
    {
      LOG(ERROR) << "Failed to remove fd=" << fd_ << " from epoll";
    }
  */
  int ret = 0;
  if (fd_ != -1) {
    ret = ::close(fd_);
    if (ret < 0) {
      LOG(ERROR) << "failed to close fd=" << fd_ << " errno=" << -errno;
    } else {
      fd_ = -1;
    }
  }
  return ret;
}

TimerNotifier::~TimerNotifier() { this->destroy(); }
}
}
