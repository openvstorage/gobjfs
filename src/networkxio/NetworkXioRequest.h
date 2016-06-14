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

#include <libxio.h>
#include <functional>
#include "NetworkXioCommon.h"

#include <list>
namespace gobjfs { namespace xio 
{

struct Work;

typedef std::function<void(Work*)> workitem_func_t;

struct Work
{
    workitem_func_t func;
    void *obj;
};

struct NetworkXioClientData;

struct NetworkXioRequest
{
    NetworkXioMsgOpcode     op;

    void                    *req_wq;

    void                    *data;
    unsigned int            data_len; // DataLen of buffer pointed by data
    size_t                  size; // Size to be written/read.
    uint64_t                offset; // at which offset

    ssize_t                 retval;
    int                     errval;
    uintptr_t               opaque;

    Work                    work;

    xio_msg *xio_req;
    xio_msg xio_reply;
    xio_reg_mem reg_mem;
    bool from_pool;

    NetworkXioClientData *pClientData;

    void    *private_data;

    std::string s_msg;
};

}} //namespace

