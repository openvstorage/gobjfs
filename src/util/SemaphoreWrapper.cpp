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

#include "SemaphoreWrapper.h"
#include <cassert>
#include <errno.h>
#include <glog/logging.h>

#define USE_SYS_SEMAPHORE

namespace gobjfs {
namespace os {

#ifdef USE_SYS_SEMAPHORE

int32_t SemaphoreWrapper::init(unsigned int initVal, int epollfd) {
  int interProcess = 0;
  int ret = sem_init(&semaphore_, interProcess, initVal);
  if (ret != 0) {
    ret = -errno;
    LOG(ERROR) << "sem init failed with errno=" << ret;
  }
  return ret;
}

int32_t SemaphoreWrapper::pause() {
  int ret = sem_wait(&semaphore_);
  if (ret != 0) {
    ret = -errno;
    LOG(ERROR) << "sem wait failed with errno=" << ret;
  }
  return ret;
}

int32_t SemaphoreWrapper::wakeup(uint64_t count) {
  int finalRet = 0;
  for (decltype(count) i = 0; i < count; i++) {
    int ret = sem_post(&semaphore_);
    if (ret != 0) {
      ret = -errno;
      LOG(ERROR) << "sem wakeup failed with errno=" << ret;
      finalRet = ret;
    }
  }
  return finalRet;
}

int SemaphoreWrapper::getValue() {
  int val = 0;
  int ret = sem_getvalue(&semaphore_, &val);
  if (ret != 0) {
    ret = -errno;
    LOG(ERROR) << "sem getvalue failed with errno=" << ret;
  } else {
    ret = val;
  }
  return ret;
}

SemaphoreWrapper::~SemaphoreWrapper() {
  int ret = sem_destroy(&semaphore_);
  if (ret != 0) {
    ret = -errno;
    LOG(ERROR) << "sem destroy failed with errno=" << ret;
  }
}

#else

int32_t SemaphoreWrapper::init(unsigned int initVal, int epollfd) {
  int flags = EFD_CLOEXEC | EFD_SEMAPHORE;
  if (epollfd != -1) {
    flags |= EFD_NONBLOCK;
  }
  fd_ = eventfd(initVal, flags);
  assert(fd_ >= 0);

  if (epollfd != -1) {
    epoll_event epollEvent;
    bzero(&epollEvent, sizeof(epollEvent));
    epollEvent.data.ptr = this;

    epollEvent.events = EPOLLIN | EPOLLPRI | EPOLLERR | EPOLLHUP;
    int retcode = epoll_ctl(epollfd, EPOLL_CTL_ADD, fd_, &epollEvent);
    assert(retcode >= 0);
    LOG(INFO) << "epoll registered semaphore fd=" << fd_
              << " with ptr=" << this;
  }
  return 0;
}

int32_t SemaphoreWrapper::pause() {
  // every read of EFD_SEMAPHORE returns a 1
  uint64_t value;
  ssize_t ret = read(fd_, &value, sizeof(value));
  assert(ret == sizeof(value));
  return 0;
}

int32_t SemaphoreWrapper::wakeup(uint64_t count) {
  assert(count != 0);
  ssize_t ret = write(fd_, &count, sizeof(count));
  int capture_errno = errno;
  assert(ret == sizeof(count));
  return 0;
}

int SemaphoreWrapper::getValue() {
  int val = 0;
  int err = sem_getvalue(&semaphore_, &val);
  if (err != 0) {
    return val;
  }
  return -errno;
}

SemaphoreWrapper::~SemaphoreWrapper() { close(fd_); }

#endif
}
}
