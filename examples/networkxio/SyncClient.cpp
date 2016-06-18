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

#include <gobjfs_client.h>
#include <networkxio/gobjfs_client_common.h> // GLOG_DEBUG

static constexpr size_t BufferSize = 4096;

int times = 10;

void NetworkServerWriteReadTest(void)
{
    auto ctx_attr = ovs_ctx_attr_new();

    ovs_ctx_attr_set_transport(ctx_attr,
                                         "tcp",
                                         "127.0.0.1",
                                         21321);

    ovs_ctx_ptr ctx = ovs_ctx_new(ctx_attr);
    assert(ctx != nullptr);

    int err = ovs_ctx_init(ctx);
    if (err < 0) {
        GLOG_ERROR("Volume open failed ");
        return;
    }

    for (int i = 0; i < times; i ++) {

      auto rbuf = (char*)malloc(BufferSize);
      assert(rbuf != nullptr);
  
      auto sz = ovs_read(ctx, "abcd", rbuf, BufferSize, 0);
  
      if (sz < 0) {
        GLOG_ERROR("OMG!!read failure with error  : " << sz);
        break;
      }
      if (sz != (ssize_t) BufferSize) {
        GLOG_ERROR("Read Length " << sz << " not matching expected " << BufferSize);
        break;
      }
      free(rbuf);
      if (i && (i % 1000 == 0)) {
        std::cout << "completed reads=" << i << std::endl;
      }
    }


}

int main(int argc, char *argv[]) {

  if (argc > 1) {
    times = atoi(argv[1]);
  }

  NetworkServerWriteReadTest();

}
