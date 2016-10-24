#include <rora/GatewayProtocol.h>
#include <rora/EdgeQueue.h>
#include <rora/ASDQueue.h>
#include <unistd.h>

using namespace gobjfs::rora;

int main(int argc, char* argv[])
{
  int pid = getpid();
  size_t maxQueueLen = 10;
  size_t maxMsgSize = 1024;
  size_t blockSize = 4096;

  // create new
  EdgeQueue* edgeQueue = new EdgeQueue(pid, maxQueueLen, maxMsgSize, blockSize);

  // open existing
  ASDQueue* asdQueue = new ASDQueue("127.0.0.1:21321");

  {
    // sending open message will cause rora gateway to open
    // the EdgeQueue for sending responses
    GatewayMsg openReq;
    createOpenRequest(openReq);
    auto ret = asdQueue->write(openReq);
    assert(ret == 0);
  }

  {
    // send read msg
    GatewayMsg readReq;
    createReadRequest(readReq, edgeQueue, "abcd", 0, blockSize);
    auto ret = asdQueue->write(readReq);
    assert(ret == 0);
  }

  {
    // get read response
    GatewayMsg responseMsg;
    auto ret = edgeQueue->read(responseMsg);
    assert(ret == 0);

    // check retval, errval, filename, offset, size match
    responseMsg.rawbuf_ = edgeQueue->segment_->get_address_from_handle(responseMsg.buf_);

    // free allocated shared segment
    edgeQueue->free(responseMsg.rawbuf_);
  }

  {
    // sending close message will cause rora gateway to close
    // the EdgeQueue for sending responses
    GatewayMsg closeReq;
    createCloseRequest(closeReq);
    auto ret = asdQueue->write(closeReq);
    assert(ret == 0);
  }

  delete asdQueue;
  delete edgeQueue;
}
