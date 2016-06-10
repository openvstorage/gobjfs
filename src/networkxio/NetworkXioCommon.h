// Copyright 2016 iNuron NV
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <iostream>
#include <sys/eventfd.h>

#define ATTR_UNUSED     __attribute__((unused))
#ifndef XXEnter
    #define XXEnter() std::cout << "Entering function " << __FUNCTION__ << " , " << __FILE__ << " ( " << __LINE__ << " ) " << std::endl;
#endif
#ifndef XXExit
    #define XXExit() std::cout << "Exiting function " << __FUNCTION__ << " , " << __FILE__ << " ( " << __LINE__ << " ) " << std::endl;
#endif
#ifndef XXDone
    #define XXDone() goto done;
#endif
#define GLOG_ERROR(msg) std::cout  << " " << __FUNCTION__ << " , " << __FILE__ << " ( " << __LINE__ << " ) " << msg << std::endl;
#define GLOG_INFO(msg) GLOG_ERROR(msg)
#define GLOG_FATAL(msg) GLOG_ERROR(msg)
#define GLOG_DEBUG(msg) GLOG_ERROR(msg)
#define GLOG_TRACE(msg) GLOG_ERROR(msg)
namespace gobjfs { namespace xio
{
static inline int
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


static inline int
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

// TODO Change
enum class NetworkXioMsgOpcode
{
    Noop,
    OpenReq,
    OpenRsp,
    CloseReq,
    CloseRsp,
    ReadReq,
    ReadRsp,
    ErrorRsp,
    ShutdownRsp,
};

#define GetNegative(err) (err > 0) ? -err:err;
