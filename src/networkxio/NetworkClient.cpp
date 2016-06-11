#include <assert.h>
#include <fcntl.h>
#include <string.h>

#include "volumedriver.h"
#include "common.h"

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

    auto rbuf = (char*)malloc(4096);
    assert(rbuf != nullptr);

    uint64_t objID = 0;
    auto sz = ovs_read(ctx, "abcd", rbuf, 4096, 0);

    if (sz < 0) {
        GLOG_ERROR("OMG!!read failure with error  : " << sz);
        return;
    }
    if (sz != (ssize_t) 4096) {
        GLOG_ERROR("Read Length and write length not matching \n");
        assert(0);
    }
    GLOG_DEBUG("\n\n------------------- ovs_read Successful -------------- \n\n");

    ovs_ctx_destroy(ctx);

    GLOG_DEBUG("\n\n------------------- ovs_ctx_destroy Successful -------------- \n\n");

    ovs_ctx_attr_destroy(ctx_attr);
}

int main(int argc, char *argv[]) {

    NetworkServerWriteReadTest();

}
