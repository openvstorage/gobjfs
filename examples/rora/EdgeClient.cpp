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
    openReq.opcode_ = Opcode::OPEN;
    openReq.edgePid_ = pid;
    auto ret = asdQueue->write(openReq);
    assert(ret == 0);
  }

  {
    // send read msg
    GatewayMsg readReq;
    auto readBuf = edgeQueue->alloc(blockSize);

    readReq.opcode_ = Opcode::READ;
    readReq.edgePid_ = pid;
    readReq.filename_ = "abcd";
    readReq.size_ = blockSize;
    readReq.offset_ = 0;
    readReq.buf_ = edgeQueue->segment_->get_handle_from_address(readBuf);

    auto ret = asdQueue->write(readReq);
    assert(ret == 0);
  }

  {
    // get read response
    GatewayMsg responseMsg;
    auto ret = edgeQueue->read(responseMsg);
    assert(ret == 0);

    // check retval, errval, filename, offset, size match
    void* respBuf = edgeQueue->segment_->get_address_from_handle(responseMsg.buf_);

    // free allocated shared segment
    edgeQueue->free(respBuf);
  }

  {
    // sending close message will cause rora gateway to close
    // the EdgeQueue for sending responses
    GatewayMsg closeReq;
    closeReq.opcode_ = Opcode::CLOSE;
    closeReq.edgePid_ = pid;
    auto ret = asdQueue->write(closeReq);
    assert(ret == 0);
  }

  delete asdQueue;
  delete edgeQueue;
}
