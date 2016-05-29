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

#include "ShutdownNotifier.h"

#include <sys/epoll.h>   // epoll
#include <sys/eventfd.h> // epoll
#include <unistd.h>      // read

#include <glog/logging.h> // LOG

#include <cassert>

namespace gobjfs {
namespace os {

int32_t ShutdownNotifier::init(int epollFD) {
  int32_t ret = 0;

  fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC | EFD_SEMAPHORE);
  if (fd_ < 0) {
    ret = -errno;
    LOG(ERROR) << "failed to create eventfd errno=" << ret;
    return ret;
  }

  epoll_event epollEvent;
  bzero(&epollEvent, sizeof(epollEvent));
  epollEvent.data.ptr = this;

  epollEvent.events = EPOLLIN | EPOLLPRI;
  ret = epoll_ctl(epollFD, EPOLL_CTL_ADD, fd_, &epollEvent);
  if (ret < 0) {
    ret = -errno;
    LOG(ERROR) << "failed to add fd=" << fd_ << " to epollfd=" << epollFD
               << " errno=" << ret;
  } else {
    LOG(INFO) << "epoll fd=" << epollFD << " registered shutdown fd=" << fd_
              << " with ptr=" << this;
  }
  return ret;
}

int32_t ShutdownNotifier::recv(uint64_t &counter) {
  int ret = eventfd_read(fd_, &counter);
  if (ret != 0) {
    ret = -errno;
    LOG(ERROR) << "failed to read fd=" << fd_ << " errno=" << ret;
  }
  return ret;
}

int32_t ShutdownNotifier::send() {
  // add one to counter
  uint64_t counter = 1;

  int ret = eventfd_write(fd_, counter);
  if (ret != 0) {
    ret = -errno;
    LOG(ERROR) << "failed to write fd=" << fd_ << " errno=" << ret;
    return ret;
  }
  return ret;
}

int32_t ShutdownNotifier::destroy() {
  /* No need to do this because epoll fd itself will be closed
    int retcode = epoll_ctl(epollFD, EPOLL_CTL_DEL, fd_, NULL);
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

ShutdownNotifier::~ShutdownNotifier() { this->destroy(); }
}
}
