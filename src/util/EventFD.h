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
#pragma once

#include <sys/eventfd.h>
#include <cstdint>
#include <unistd.h>


// cannot use namespace because this class used in typedef in gIOExecFile.h

struct EventFD {
  EventFD() {
    evfd_ = eventfd(0, EFD_NONBLOCK);
    if (evfd_ < 0) {
      throw std::runtime_error("failed to create eventfd");
    }
  }

  ~EventFD() {
    if (evfd_ != -1) {
      close(evfd_);
    }
  }

  EventFD(const EventFD &) = delete;

  EventFD &operator=(const EventFD &) = delete;

  operator int() const { return evfd_; }

  static int readfd(int fd) {
    int ret;
    eventfd_t value = 0;
    do {
      ret = eventfd_read(fd, &value);
    } while (ret < 0 && errno == EINTR);
    if (ret == 0) {
      ret = value;
    } else if (errno != EAGAIN) {
      abort();
    }
    return ret;
  }

  int readfd() {
    return readfd(evfd_);
  }

  int writefd() {
    uint64_t u = 1;
    int ret;
    do {
      ret = eventfd_write(evfd_, static_cast<eventfd_t>(u));
    } while (ret < 0 && (errno == EINTR || errno == EAGAIN));
    if (ret < 0) {
      abort();
    }
    return ret;
  }

private:
  int evfd_;
};

