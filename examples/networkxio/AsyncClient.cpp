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
#include <future>
#include <thread>

#include <gobjfs_client.h>
#include <networkxio/gobjfs_client_common.h> // GLOG_DEBUG


using namespace gobjfs::xio;

int32_t times = 1000;
client_ctx_ptr ctx;

void iocompletionFunc() {

  int32_t doneCount = 0;

  while (doneCount < times) {

    std::vector<giocb *> iocb_vec;

    int r = aio_getevents(ctx, times, iocb_vec);

    if (r == 0) {
      for (auto &iocb : iocb_vec) {
        aio_finish(iocb);
        std::free(iocb->aio_buf);
        delete iocb; 
      }
      doneCount += iocb_vec.size();
      std::cout << "finished=" << iocb_vec.size() << ",total=" << doneCount << std::endl;
    }
  }
}

void NetworkServerWriteReadTest() {

  for (int i = 0; i < times; i++) {

    auto rbuf = (char *)malloc(4096);
    assert(rbuf != nullptr);

    giocb *iocb = new giocb;
    iocb->filename = "abcd";
    iocb->aio_buf = rbuf;
    iocb->aio_offset = 0;
    iocb->aio_nbytes = 4096;

    auto ret = aio_read(ctx, iocb);

    if (ret != 0) {
      std::free(iocb->aio_buf);
      delete iocb;
    } 
  }
}

int main(int argc, char *argv[]) {

  times = atoi(argv[1]);

  auto ctx_attr = ctx_attr_new();
  ctx_attr_set_transport(ctx_attr, "tcp", "127.0.0.1", 21321);

  ctx = ctx_new(ctx_attr);
  assert(ctx != nullptr);

  int err = ctx_init(ctx);
  assert(err == 0);

  NetworkServerWriteReadTest();

  auto fut = std::async(std::launch::async,
      iocompletionFunc);

  aio_wait_all(ctx);

  fut.wait();

  std::cout << ctx_get_stats(ctx) << std::endl;

  ctx.reset();
}
