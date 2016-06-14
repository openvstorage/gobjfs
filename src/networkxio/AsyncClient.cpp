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

#include "volumedriver.h"
#include "common.h"


/*
DISABLED - where NetworkXioClient calls the completion?
void callback_func(ovs_completion_t *cb, void* arg)
{
  free(arg); 
  ovs_aio_release_completion(cb);
  std::cout << "callback with " << (void*)cb << ":" << (void*)arg << std::endl;
}
*/


void NetworkServerWriteReadTest(void)
{
    ovs_ctx_attr_t *ctx_attr = ovs_ctx_attr_new();

    ovs_ctx_attr_set_transport(ctx_attr,
                                         "tcp",
                                         "127.0.0.1",
                                         21321);

    ovs_ctx_t *ctx = ovs_ctx_new(ctx_attr);
    assert(ctx != nullptr);

    int err = ovs_ctx_init(ctx,
               "/dev/sdb",
               O_RDWR);
    if (err < 0) {
        GLOG_ERROR("Volume open failed ");
        return;
    }

    std::vector<ovs_aiocb*> vec;

    for (int i = 0; i < 1000; i ++) {

      auto rbuf = (char*)malloc(4096);
      assert(rbuf != nullptr);


      ovs_aiocb* iocb = (ovs_aiocb*)malloc(sizeof(ovs_aiocb));
      iocb->aio_buf = rbuf;
      iocb->aio_offset = 0;
      iocb->aio_nbytes = 4096;
/*
      ovs_completion_t* comp = ovs_aio_create_completion(callback_func, iocb);
  
      auto ret = ovs_aio_readcb(ctx, "abcd", iocb, comp);

      if (ret == 0) {
        ovs_aio_wait_completion(comp, nullptr);
        std::cout << "waiting for req=" << i << std::endl;
      } else {
        free(iocb);
        ovs_aio_release_completion(comp);
      }
*/
      auto ret = ovs_aio_read(ctx, "abcd", iocb);
      if (ret != 0) {
        free(iocb);
        free(rbuf);
      } else {
        vec.push_back(iocb);
      }
    }

    for (auto& elem : vec) {
      ovs_aio_suspend(ctx, elem, nullptr);
      ovs_aio_finish(ctx, elem);
      free(elem->aio_buf);
      free(elem);
    }

    ovs_ctx_destroy(ctx);

    GLOG_DEBUG("\n\n------------------- ovs_ctx_destroy Successful -------------- \n\n");

    ovs_ctx_attr_destroy(ctx_attr);
}

int main(int argc, char *argv[]) {

    NetworkServerWriteReadTest();

}
