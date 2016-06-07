#include "NetworkXioInterface.h"
#include <assert.h>
#include <volumedriver.h>

void NetworkServerWriteReadTest(void)
{
    uint64_t volume_size = 1 << 30;
    ovs_ctx_attr_t *ctx_attr = ovs_ctx_attr_new();
              ovs_ctx_attr_set_transport(ctx_attr,
                                         "tcp",
                                         "127.0.0.1",
                                         21321);
    ovs_ctx_t *ctx = ovs_ctx_new(ctx_attr);
    assert(ctx != nullptr);
    /*ovs_create_volume(ctx,
                    "volume",
                    volume_size);
    GLOG_DEBUG("\n\n------------------- Create Volume Successful -------------- \n\n");*/
    int err = ovs_ctx_init(ctx,
               "/dev/sdb",
               O_RDWR);
    if (err < 0) {
        GLOG_ERROR("Volume open failed ");
        return;
    }

    GLOG_DEBUG("\n\n------------------- Volume open Successful -------------- \n\n");

    std::string pattern("DCENGINES DCENGINES");
    auto wbuf = std::make_unique<uint8_t[]>(pattern.length());
    assert(wbuf != nullptr);

    memcpy(wbuf.get(), pattern.c_str(), pattern.length());

    uint64_t objID = 1234;
    auto sz = ovs_write(ctx, objID, wbuf.get(), 
                        pattern.length(),
                        1024);
    if (sz < 0) {
        GLOG_ERROR("OMG!!Write failure with error  : " << sz);
        return;
    }

    if (sz != (ssize_t)pattern.length()) {
        GLOG_ERROR("ovs_write() returned " << sz << " but pattern length not matching. len is " << pattern.length());
    }
    GLOG_DEBUG("\n\n------------------- ovs_write Successful -------------- \n\n");

    /*int ret = ovs_flush(ctx, objID);
    assert (ret == 0);

    wbuf.reset();
    GLOG_DEBUG("\n\n------------------- ovs_flush Successful -------------- \n\n");

    
    auto rbuf = std::make_unique<uint8_t[]>(pattern.length());
    assert(rbuf != nullptr);

    sz = ovs_read(ctx,  objID, rbuf.get(), pattern.length(), 1024);

    if (sz < 0) {
        GLOG_ERROR("OMG!!Write failure with error  : " << sz);
        return;
    }
    if (sz != (ssize_t) pattern.length()) {
        GLOG_ERROR("Read Length and write length not matching \n");
        assert(0);
    }
    ret = memcmp(rbuf.get(), pattern.c_str(), pattern.length());
    if (ret != 0) {
        GLOG_ERROR("memcmp failed \n");
        assert(0);
    }

    rbuf.reset();

    GLOG_DEBUG("\n\n------------------- ovs_read Successful -------------- \n\n");
    */
    ovs_ctx_destroy(ctx);
    GLOG_DEBUG("\n\n------------------- ovs_ctx_destroy Successful -------------- \n\n");
    ovs_ctx_attr_destroy(ctx_attr);
}
int main(int argc, char *argv[]) {
/*
    ovs_ctx_attr_t *ctx_attr = ovs_ctx_attr_new();
    assert(ctx_attr != nullptr);
    ovs_ctx_attr_set_transport(ctx_attr,"tcp","127.0.0.1", 21321);
    ovs_ctx_t *ctx = ovs_ctx_new(ctx_attr);
    assert(ctx != nullptr);
    ovs_create_volume(ctx,"volume",10000);
    ovs_ctx_attr_destroy(ctx_attr);
*/
    NetworkServerWriteReadTest();
}
