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
  ASDQueue* asdQueue = new ASDQueue("127.0.0.1:21321", 1);

  {
    // sending open message will cause rora gateway to open
    // the EdgeQueue for sending responses
    auto ret = asdQueue->write(createOpenRequest());
    assert(ret == 0);
  }

  {
    // send read msg
    auto ret = asdQueue->write(createReadRequest(edgeQueue, 1, "abcd", 0, blockSize));
    assert(ret == 0);
  }

  {
    // get read response
    GatewayMsg responseMsg;
    auto ret = edgeQueue->readResponse(responseMsg);
    assert(ret == 0);

    // check retval, errval, filename, offset, size match
    responseMsg.rawbufVec_.push_back(edgeQueue->segment_->get_address_from_handle(responseMsg.bufVec_[0]));

    // free allocated shared segment
    for (auto ptr : responseMsg.rawbufVec_) {
      edgeQueue->free(ptr);
    }
  }

  {
    // sending close message will cause rora gateway to close
    // the EdgeQueue for sending responses
    auto ret = asdQueue->write(createCloseRequest());
    assert(ret == 0);
  }

  delete asdQueue;
  delete edgeQueue;
}
