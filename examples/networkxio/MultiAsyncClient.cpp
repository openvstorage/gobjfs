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

void NetworkServerWriteReadTest()
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

    std::vector<ovs_aiocb*> iocb_vec;

    for (int i = 0; i < times; i ++) {

      auto rbuf = (char*)malloc(4096);
      assert(rbuf != nullptr);

      ovs_aiocb* iocb = (ovs_aiocb*)malloc(sizeof(ovs_aiocb));
      iocb->aio_buf = rbuf;
      iocb->aio_offset = times * 4096;
      iocb->aio_nbytes = 4096;

      iocb_vec.push_back(iocb);
    }

    auto ret = ovs_aio_readv(ctx, "abcd", iocb_vec);

    if (ret == 0) {
      ret = ovs_aio_suspendv(ctx, iocb_vec, nullptr); 
    } 

    for (auto& elem : iocb_vec) {
      ovs_aio_finish(ctx, elem);
      free(elem->aio_buf);
      free(elem);
    }


    GLOG_DEBUG("\n\n------------------- ovs_ctx_destroy Successful -------------- \n\n");

}

int main(int argc, char *argv[]) {

    times = atoi(argv[1]);

    NetworkServerWriteReadTest();

}
