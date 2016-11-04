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

#include <cstdint>

#include <util/Stats.h>


// cannot use namespace because this class used in typedef in gIOExecFile.h

struct EventFD {

  struct Statistics {
    uint64_t eintr_{0}; // how many times EINTR hit
    uint64_t eagain_{0}; // how many times EAGAIN hit 
    gobjfs::stats::StatsCounter<int64_t> ctr_; // average read value

    void clear();
  } stats_;

  EventFD();

  ~EventFD();

  EventFD(const EventFD &) = delete;

  EventFD &operator=(const EventFD &) = delete;

  operator int() const { return evfd_; }

  int getfd() const { return evfd_; }

  // made func static so it can be called when EventFD object not available
  static int readfd(int fd, EventFD* evfd = nullptr);

  int readfd();

  int writefd();

private:

  int evfd_{-1};

};

