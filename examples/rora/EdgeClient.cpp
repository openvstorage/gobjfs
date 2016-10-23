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
  ASDQueue* asdQueue = new ASDQueue("127.0.0.1:12321");

  {
    // sending open message will cause rora gateway to open
    // the EdgeQueue for sending responses
    GatewayMsg openReq;
    openReq.opcode_ = Opcode::OPEN;
    openReq.edgePid_ = pid;
    auto openStr = openReq.pack();
    ssize_t ret = asdQueue->write(openStr.c_str(), openStr.size());
    assert(ret == (ssize_t)openStr.size());
  }

  {
    // send read msg
    GatewayMsg readReq;
    readReq.opcode_ = Opcode::READ;
    readReq.edgePid_ = pid;
    readReq.filename_ = "abcd";
    readReq.size_ = blockSize;
    readReq.offset_ = 0;
    auto readBuf = edgeQueue->alloc(blockSize);
    readReq.buf_ = edgeQueue->segment_->get_handle_from_address(readBuf);
    auto readStr = readReq.pack();
    ssize_t ret = asdQueue->write(readStr.c_str(), readStr.size());
    assert(ret == (ssize_t)readStr.size());
  }

  {
    // get read response
    char msg[maxMsgSize];
    ssize_t readSz = edgeQueue->read(msg, maxMsgSize);

    GatewayMsg responseMsg;
    responseMsg.unpack(msg, readSz);

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
    std::string closeStr = closeReq.pack();
    ssize_t ret = asdQueue->write(closeStr.c_str(), closeStr.size());
    assert(ret == (ssize_t)closeStr.size());
  }

  delete asdQueue;
  delete edgeQueue;
}
