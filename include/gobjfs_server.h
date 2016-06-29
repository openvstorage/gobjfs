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

#include <stdbool.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/types.h>

//#include <gIOExecFile.h>

typedef void* gobjfs_xio_server_handle;
typedef int (* FileTranslatorFunc)(const char*, size_t, char*);
// C API
#ifdef __cplusplus
extern "C" {
#endif

// @param transport : "tcp" or "rdma"
// @param host : Host string FQDN or IP address
// @param port : TCP/RDMA port
// @param number_cores : CPU cores on which to start IOExecutor
// @param queue_depth : kernel queue depth to allocate for async io
// @param is_new_instance : cleans up old directories
// @return server_handle OR nullptr
gobjfs_xio_server_handle gobjfs_xio_server_start(
  const char* transport,
  const char* host,
  int port,
  int32_t number_cores,
  int32_t queue_depth,
  FileTranslatorFunc file_translator_func,
  bool is_new_instance);

// @param server_handle : handle returned from "server_start"
// @return 0 on success, else negative error code
int gobjfs_xio_server_stop(
  gobjfs_xio_server_handle server_handle);

typedef enum { TRACE,DEBUG, INFO, WARNING, ERROR, FATAL} gobjfs_log_level;
void gobjfs_init_logging(gobjfs_log_level level);

#ifdef __cplusplus
}
#endif
