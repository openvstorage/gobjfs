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

#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/types.h>

// C API
#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

struct gobjfs_xio_server_int;
typedef gobjfs_xio_server_int* gobjfs_xio_server_handle;

EXTERNC {

// @param transport : "tcp" or "rdma"
// @param host : Host string FQDN or IP address
// @param port : TCP/RDMA port
// @param config_file : config contains ioexec and file_distributor options
// @param is_new_instance : cleans up old directories
// @return server_handle OR nullptr
gobjfs_xio_server_handle gobjfs_xio_server_start(
  const char* transport, 
  const char* host, 
  int port,
  const char* config_file,
  bool is_new_instance);

// @param server_handle : handle returned from "server_start"
// @return 0 on success, else negative error code
int gobjfs_xio_server_stop(
  gobjfs_xio_server_handle server_handle);

}
