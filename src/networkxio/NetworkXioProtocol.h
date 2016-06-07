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

#ifndef __NETWORK_XIO_PROTOCOL_H_
#define __NETWORK_XIO_PROTOCOL_H_

#include <msgpack.hpp>
#include <sstream>
#include "NetworkXioCommon.h"
#include <sys/epoll.h>
class NetworkXioMsg
{
public:
        NetworkXioMsgOpcode opcode_;
        std::string         devname_;
        uint64_t         gobjid_;
        uint64_t            offset_;
        size_t              size_;
        ssize_t             retval_;
        int                 errval_;
        uintptr_t           opaque_;
        int64_t             timeout_;

public:
    explicit NetworkXioMsg(NetworkXioMsgOpcode opcode =
                                NetworkXioMsgOpcode::Noop,
                           const std::string& devname = "",
                           const uint64_t gobjid = 0,
                           const size_t size = 0,
                           const uint32_t contid = 0,
                           const uint64_t offset = 0,
                           const ssize_t retval = 0,
                           const int errval = 0,
                           const uintptr_t opaque = 0,
                           const int64_t timeout = 0)
    : opcode_(opcode)
    , devname_(devname)
    , gobjid_ (gobjid)
    , size_(size)
    , offset_(offset)
    , retval_(retval)
    , errval_(errval)
    , opaque_(opaque)
    , timeout_(timeout)
    {}


public:
    const NetworkXioMsgOpcode&
    opcode() const
    {
        return opcode_;
    }

    void
    opcode(const NetworkXioMsgOpcode& op)
    {
        opcode_ = op;
    }

    const std::string&
    device_name() const
    {
        return devname_;
    }

    void
    device_name(const std::string& deviceName)
    {
        devname_ = deviceName;
    }

    const uintptr_t&
    opaque() const
    {
        return opaque_;
    }

    void
    opaque(const uintptr_t& opq)
    {
        opaque_ = opq;
    }

    const size_t&
    size() const
    {
        return size_;
    }

    void
    size(const size_t& size)
    {
        size_ = size;
    }

    const ssize_t&
    retval() const
    {
        return retval_;
    }

    void
    retval(const ssize_t& retval)
    {
        retval_ = retval;
    }

    const int&
    errval() const
    {
        return errval_;
    }

    void
    errval(const int& errval)
    {
        errval_ = errval;
    }

    const uint64_t&
    offset() const
    {
        return offset_;
    }

    
    void
    offset(const uint64_t& offset)
    {
        offset_ = offset;
    }

    const int64_t&
    timeout() const
    {
        return timeout_;
    }

    void
    timeout(const int64_t& timeout)
    {
        timeout_ = timeout;
    }

    const std::string
    pack_msg() const
    {
        std::stringstream sbuf;
        msgpack::pack(sbuf, *this);
        return sbuf.str();
    }

    void
    unpack_msg(const char *sbuf,
               const size_t size)
    {
        msgpack::unpacked msg;
        msgpack::unpack(&msg, sbuf, size);
        msgpack::object obj = msg.get();
        obj.convert(this);
    }

    void
    unpack_msg(const std::string& sbuf)
    {
        msgpack::unpacked msg;
        msgpack::unpack(&msg, sbuf.data(), sbuf.size());
        msgpack::object obj = msg.get();
        obj.convert(this);
    }

    void
    clear()
    {
        opcode_ = NetworkXioMsgOpcode::Noop;
        devname_.clear();
        gobjid_ = 0;
        offset_ = 0;
        retval_ = 0;
        errval_ = 0;
        opaque_ = 0;
        timeout_ = 0;
    }
public:
    MSGPACK_DEFINE(opcode_,
                   devname_,
                   gobjid_,
                   size_,
                   offset_,
                   retval_,
                   errval_,
                   opaque_,
                   timeout_);
};

MSGPACK_ADD_ENUM(NetworkXioMsgOpcode);

#endif //__NETWORK_XIO_PROTOCOL_H_
