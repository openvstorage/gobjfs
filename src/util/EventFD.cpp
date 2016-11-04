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

#include <util/EventFD.h>
#include <sys/eventfd.h>
#include <unistd.h>

void EventFD::Statistics::clear() {
  ctr_.reset();
  eagain_ = 0;
}

EventFD::EventFD() {
  evfd_ = eventfd(0, EFD_NONBLOCK);
  if (evfd_ < 0) {
    throw std::runtime_error("failed to create eventfd");
  }
}

EventFD::~EventFD() {
  if (evfd_ != -1) {
    close(evfd_);
    evfd_ = -1;
  }
}

// made func static so it can be called when EventFD object not available
int EventFD::readfd(int fd, EventFD* evfd) {
  int ret = -1;
  eventfd_t value = 0;
  int32_t numIntrLoops = 0;
  do {
    ret = eventfd_read(fd, &value);
    numIntrLoops ++;
  } while (ret < 0 && errno == EINTR);

  if (evfd) {
    evfd->stats_.eintr_ = numIntrLoops;
  }

  if (ret == 0) {
    ret = value;
    if (evfd) { 
      evfd->stats_.ctr_ = value; 
    }
  } else if (errno != EAGAIN) {
    throw std::runtime_error("failed to read eventfd=" + std::to_string(fd));
  } else {
    // it must be eagain !
    if (evfd) { 
      evfd->stats_.eagain_ ++;
    }
  }
  return ret;
}

int EventFD::readfd() {
  return readfd(evfd_, this);
}

int EventFD::writefd() {
  uint64_t u = 1;
  int ret = 0;
  do {
    ret = eventfd_write(evfd_, static_cast<eventfd_t>(u));
  } while (ret < 0 && (errno == EINTR || errno == EAGAIN));
  if (ret < 0) {
    throw std::runtime_error("failed to write eventfd=" + std::to_string(evfd_));
  }
  return ret;
}
