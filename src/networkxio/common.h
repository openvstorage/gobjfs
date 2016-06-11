// This file is dual licensed GPLv2 and Apache 2.0.
// Active license depends on how it is used.
//
// Copyright 2016 iNuron NV
//
// // GPL //
// This file is part of OpenvStorage.
//
// OpenvStorage is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with OpenvStorage. If not, see <http://www.gnu.org/licenses/>.
//
// // Apache 2.0 //
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#define ATTRIBUTE_UNUSED __attribute__((unused))

#define ATTRIBUTE_UNUSED     __attribute__((unused))

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

// TODO
enum class RequestOp
{
    Noop,
    Read,
    Open,
    Close,
};

enum class TransportType
{
    Error,
    SharedMemory,
    TCP,
    RDMA,
};

struct ovs_context_attr_t
{
    TransportType transport;
    char *host{nullptr};
    int port{-1};
};

struct ovs_buffer
{
    void *buf{nullptr};
    size_t size;
};

struct ovs_completion
{
    ovs_callback_t complete_cb;
    void *cb_arg{nullptr};
    bool _on_wait{false};
    bool _calling{false};
    bool _signaled{false};
    bool _failed{false};
    ssize_t _rv{0};
    pthread_cond_t _cond;
    pthread_mutex_t _mutex;
};

struct ovs_aio_request
{
    struct ovs_aiocb *ovs_aiocbp{nullptr};
    ovs_completion_t *completion{nullptr};
    RequestOp _op{RequestOp::Noop};
    bool _on_suspend{false};
    bool _canceled{false};
    bool _completed{false};
    bool _signaled{false};
    bool _failed{false};
    int _errno{0};
    ssize_t _rv{0};
    pthread_cond_t _cond;
    pthread_mutex_t _mutex;
};

