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

void NetworkServerWriteReadTest(void)
{
    ovs_ctx_attr_t *ctx_attr = ovs_ctx_attr_new();

    ovs_ctx_attr_set_transport(ctx_attr,
                                         "tcp",
                                         "127.0.0.1",
                                         21321);

    ovs_ctx_t *ctx = ovs_ctx_new(ctx_attr);
    assert(ctx != nullptr);

    int err = ovs_ctx_init(ctx);
    if (err < 0) {
        GLOG_ERROR("Volume open failed ");
        return;
    }

    for (int i = 0; i < 100000; i ++) {

      auto rbuf = (char*)malloc(4096);
      assert(rbuf != nullptr);
  
      auto sz = ovs_read(ctx, "abcd", rbuf, 4096, 0);
  
      if (sz < 0) {
        GLOG_ERROR("OMG!!read failure with error  : " << sz);
        break;
      }
      if (sz != (ssize_t) 4096) {
        GLOG_ERROR("Read Length and write length not matching \n");
        break;
      }
      free(rbuf);
    }

    ovs_ctx_destroy(ctx);

    GLOG_DEBUG("\n\n------------------- ovs_ctx_destroy Successful -------------- \n\n");

    ovs_ctx_attr_destroy(ctx_attr);
}

int main(int argc, char *argv[]) {

    NetworkServerWriteReadTest();

}
