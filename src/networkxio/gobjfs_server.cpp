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

#include <cerrno>
#include <limits.h>
#include <map>

#include <gobjfs_server.h>
#include <networkxio/NetworkXioCommon.h>
#include <networkxio/NetworkXioServer.h>

using gobjfs::xio::NetworkXioServer;

struct gobjfs_xio_server_int
{
  NetworkXioServer* server{nullptr};
  std::future<void> future;

  gobjfs_xio_server_int(NetworkXioServer* xs)
  {
    server = xs;
  }
  ~gobjfs_xio_server_int()
  {
  }
};

gobjfs_xio_server_handle gobjfs_xio_server_start(
  const char* transport,
  const char* host,
  int port,
  int32_t number_cores,
  int32_t queue_depth,
  FileTranslatorFunc file_translator_func,
  bool is_new_instance)
{
  const std::string uri = transport + std::string("://") +
    host + std::string(":") + std::to_string(port);

  NetworkXioServer* xs = new NetworkXioServer(uri,
    number_cores,
    queue_depth,
    file_translator_func,
    is_new_instance);

  std::promise<void> pr;
  auto init_future = pr.get_future();

  gobjfs_xio_server_int* s = new gobjfs_xio_server_int(xs);

  s->future = std::async(std::launch::async,
    [&] () { xs->run(pr); });

  init_future.wait();

  return s;
}

int gobjfs_xio_server_stop(
  gobjfs_xio_server_handle server_handle)
{
  gobjfs_xio_server_int* handle = (gobjfs_xio_server_int*) server_handle;
  handle -> server->shutdown();

  handle -> future.wait();

  delete handle;

  return 0;
}
