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

#include <iostream>
#include <sys/eventfd.h>

namespace gobjfs { namespace xio
{

inline int
xeventfd_read(int fd)
{
    int ret;
    eventfd_t value = 0;
    do {
        ret = eventfd_read(fd, &value);
    } while (ret < 0 && errno == EINTR);
    if (ret == 0)
    {
        ret = value;
    }
    else if (errno != EAGAIN)
    {
        abort();
    }
    return ret;
}


inline int
xeventfd_write(int fd)
{
    uint64_t u = 1;
    int ret;
    do {
        ret = eventfd_write(fd, static_cast<eventfd_t>(u));
    } while (ret < 0 && (errno == EINTR || errno == EAGAIN));
    if (ret < 0)
    {
        abort();
    }
    return ret;
}

}} //namespace

#define GetNegative(err) (err > 0) ? -err:err;
