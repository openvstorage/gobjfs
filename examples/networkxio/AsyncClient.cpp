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
#include <fcntl.h>
#include <string.h>
#include <vector>

#include <gobjfs_client.h>
#include <networkxio/common.h>
#include <networkxio/context.h>

int times = 10;


void callback_func(ovs_completion_t *cb, void* arg)
{
  GLOG_DEBUG( "callback with " << (void*)cb << ":" << (void*)arg);
}


void NetworkServerWriteReadTest(bool use_completion)
{
  Context::Attr ctx_attr;

    ctx_attr.setTransport("tcp",
                                         "127.0.0.1",
                                         21321);

    Context ctx(ctx_attr);

    int err = ovs_ctx_init(&ctx,
               "/dev/sdb",
               O_RDWR);

    if (err < 0) {
        GLOG_ERROR("Volume open failed ");
        return;
    }

    std::vector<ovs_aiocb*> vec;
    std::vector<ovs_completion_t*> cvec;

    for (int i = 0; i < times; i ++) {

      auto rbuf = (char*)malloc(4096);
      assert(rbuf != nullptr);


      ovs_aiocb* iocb = (ovs_aiocb*)malloc(sizeof(ovs_aiocb));
      iocb->aio_buf = rbuf;
      iocb->aio_offset = 0;
      iocb->aio_nbytes = 4096;

      ovs_completion_t* comp = nullptr;

      if (use_completion) {
        comp = ovs_aio_create_completion(callback_func, iocb);
      }

      auto ret = ovs_aio_readcb(&ctx, "abcd", iocb, comp);

      if (ret != 0) {
        free(iocb);
        free(rbuf);
        if (comp)
          ovs_aio_release_completion(comp);
      } else {
        if (comp) 
          cvec.push_back(comp);
        vec.push_back(iocb);
      }
    }

    // if completions are in use, wait for them
    for (auto& elem : cvec) {
      ovs_aio_wait_completion(elem, nullptr);
      ovs_aio_release_completion(elem);
    }

    for (auto& elem : vec) {
      // if completions not used, call suspend
      if (!use_completion) 
        ovs_aio_suspend(&ctx, elem, nullptr); 
      ovs_aio_finish(&ctx, elem);
      free(elem->aio_buf);
      free(elem);
    }

    GLOG_DEBUG("\n\n------------------- ovs_ctx_destroy Successful -------------- \n\n");
}

int main(int argc, char *argv[]) {

    times = atoi(argv[1]);

    // To use completions, pass non-zero 2nd arg
    bool use_completion = (atoi(argv[2]) > 0) ? true : false;

    NetworkServerWriteReadTest(use_completion);

}
