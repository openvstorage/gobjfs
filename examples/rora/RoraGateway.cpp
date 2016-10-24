#include <rora/GatewayProtocol.h>
#include <rora/EdgeQueue.h>
#include <rora/ASDQueue.h>

#include <gobjfs_client.h>

#include <glog/logging.h>
#include <unistd.h>
#include <thread>
#include <future>

using namespace gobjfs::rora;
using namespace gobjfs::xio;

std::map<int, EdgeQueue*> edgeCatalog;

client_ctx_ptr ctx;
int times = 1;

void iocompletionFunc() {

  int32_t doneCount = 0;

  while (doneCount < times) {
    std::vector<giocb*> iocb_vec;
    int r = aio_getevents(ctx, times, iocb_vec);

    if (r == 0) {
      for (auto& iocb : iocb_vec) {
        aio_finish(iocb);
        //int pid = (int) iocb->userCtx_;
        int pid = 10;
        auto edgeIter = edgeCatalog.find(pid);
        auto edgeQueue = edgeIter->second;

        GatewayMsg respMsg;
        respMsg.filename_ = iocb->filename;
        respMsg.offset_ = iocb->aio_offset;
        respMsg.offset_ = iocb->aio_offset;
        respMsg.size_ = iocb->aio_nbytes;
        auto sendStr = respMsg.pack();
        edgeQueue->write(sendStr.c_str(), sendStr.size());
      }
      doneCount = iocb_vec.size();
    }
  }
}

int main(int argc, char* argv[])
{
  size_t maxQueueLen = 10;
  size_t maxMsgSize = 1024;

  auto ctx_attr = ctx_attr_new();
  ctx_attr_set_transport(ctx_attr, "tcp", "127.0.0.1", 21321);

  ctx = ctx_new(ctx_attr);
  assert(ctx != nullptr);

  int err = ctx_init(ctx);
  assert(err == 0);

  // open new
  ASDQueue* asdQueue = new ASDQueue("127.0.0.1:12321", maxQueueLen, maxMsgSize);

  auto fut = std::async(std::launch::async, iocompletionFunc);

  while (1) 
  {
    char buf [maxMsgSize];
    ssize_t recvdSize = asdQueue->read(buf, maxMsgSize);

    GatewayMsg anyReq;
    anyReq.unpack(buf, recvdSize);

    switch (anyReq.opcode_) {
      case Opcode::OPEN:
        {
          EdgeQueue* newEdge = new EdgeQueue(anyReq.edgePid_);
          edgeCatalog.insert(std::make_pair(anyReq.edgePid_, newEdge));
          break;
        }
      case Opcode::READ:
        {
          giocb* iocb = new giocb;
          iocb->filename = anyReq.filename_;
          iocb->aio_offset = anyReq.offset_;
          iocb->aio_nbytes = anyReq.size_;
          int pid = 10;
          // int pid = (pid_t)iocb->userCtx_;
          auto edgeIter = edgeCatalog.find(pid);
          // TODO check null
          auto edgeQueue = edgeIter->second;
          iocb->aio_buf = edgeQueue->segment_->get_address_from_handle(anyReq.buf_);
          //iocb->userCtx_ = anyReq.edgePid_;
          auto aio_ret = aio_read(ctx, iocb);
          if (aio_ret != 0) {
            // TODO edgeQueue->write response
            delete iocb;
          }
        }
      case Opcode::CLOSE:
        {
          size_t sz = edgeCatalog.erase(anyReq.edgePid_);
          if (sz != 1) {
          LOG(ERROR) << "could not delete edgeQUeue for pid=" << anyReq.edgePid_;
          }
          break;
        }
      default:
        break;
    }
  }

  aio_wait_all(ctx);
  fut.wait();

  delete asdQueue;

  for (auto& edgeIter : edgeCatalog) {
    delete edgeIter.second;
  }
}
