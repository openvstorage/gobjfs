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

#include <atomic>
#include <list>

#include "NetworkXioCommon.h"

namespace gobjfs { namespace xio 
{

class NetworkXioServer;
class NetworkXioIOHandler;

struct NetworkXioClientData
{
    xio_session *ncd_session{nullptr};
    xio_connection *ncd_conn{nullptr};
    xio_mempool *ncd_mpool{nullptr};
    std::atomic<bool> ncd_disconnected{false};
    std::atomic<uint64_t> ncd_refcnt{0};
    NetworkXioServer *ncd_server{nullptr};
    NetworkXioIOHandler *ncd_ioh{nullptr};
    std::list<NetworkXioRequest*> ncd_done_reqs;

    NetworkXioClientData(xio_mempool* pool, 
      NetworkXioServer* server,
      xio_session* session,
      xio_connection* conn)
      : ncd_session(session)
      , ncd_conn(conn)
      , ncd_mpool(pool)
      , ncd_server(server) 
    { }
};

}} //namespace

