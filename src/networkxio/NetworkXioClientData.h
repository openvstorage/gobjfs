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

#include <atomic>
#include <list>

#include "NetworkXioWork.h"
#include "NetworkXioCommon.h"

namespace gobjfs { namespace xio 
{

class NetworkXioServer;
class NetworkXioIOHandler;

struct NetworkXioClientData
{
    xio_session *ncd_session;
    xio_connection *ncd_conn;
    xio_mempool *ncd_mpool;
    std::atomic<bool> ncd_disconnected;
    std::atomic<uint64_t> ncd_refcnt;
    NetworkXioServer *ncd_server;
    NetworkXioIOHandler *ncd_ioh;
    std::list<NetworkXioRequest*> ncd_done_reqs;
};

}} //namespace

