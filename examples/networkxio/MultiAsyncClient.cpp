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
#include <assert.h>
#include <iostream>
#include <fcntl.h>
#include <string.h>
#include <vector>

#include <gobjfs_client.h>
#include <networkxio/gobjfs_client_common.h> // GLOG_DEBUG

int times = 10;
int batchSize = 10;
int numports = 2;

using namespace gobjfs::xio;

void NetworkServerWriteReadTest() {

  std::vector<gobjfs::xio::client_ctx_ptr> ctx_ptr_vec;

  for (int idx = 0; idx < numports; idx ++) {

    auto ctx_attr = ctx_attr_new();
    ctx_attr_set_transport(ctx_attr, "tcp", "127.0.0.1", 21321 + (100 * idx));
    client_ctx_ptr ctx = ctx_new(ctx_attr);
    assert(ctx != nullptr);
    int err = ctx_init(ctx);
    assert(err == 0);
    ctx_ptr_vec.push_back(ctx);
  }


  for (int i = 0; i < times; i++) {

    std::vector<giocb *> iocb_vec;

    for (int j = 0; j < batchSize; j++) {

      auto rbuf = (char *)malloc(4096);
      assert(rbuf != nullptr);

      giocb *iocb = new giocb;
      iocb->filename = "abcd";
      iocb->aio_buf = rbuf;
      iocb->aio_offset = j * 4096;
      iocb->aio_nbytes = 4096;

      iocb_vec.push_back(iocb);
    }

    int32_t slot = i % ctx_ptr_vec.size();
    auto ctx = ctx_ptr_vec.at(slot);
    auto ret = aio_readv(ctx, iocb_vec);

    if (ret == 0) {
      ret = aio_suspendv(ctx, iocb_vec, nullptr);
    }

    for (auto &iocb : iocb_vec) {
      aio_finish(ctx, iocb);
      free(iocb->aio_buf);
      delete iocb;
    }
  }

  for (auto& ctx : ctx_ptr_vec) {
    std::cout << ctx_get_stats(ctx) << std::endl;
  }
}

int main(int argc, char *argv[]) {

  if (argc > 1) {
    times = atoi(argv[1]);
  }

  NetworkServerWriteReadTest();
}
