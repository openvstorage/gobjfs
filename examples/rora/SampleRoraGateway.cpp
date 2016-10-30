#include <rora/GatewayProtocol.h>
#include <rora/EdgeQueue.h>
#include <rora/ASDQueue.h>

#include <gobjfs_client.h>

#include <gobjfs_log.h>
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

        int pid = (int) iocb->user_ctx;

        auto edgeIter = edgeCatalog.find(pid);
        if (edgeIter != edgeCatalog.end()) {
          auto edgeQueue = edgeIter->second;

          GatewayMsg respMsg;
          edgeQueue->GatewayMsg_from_giocb(respMsg, *iocb, 
              aio_return(iocb), aio_error(iocb));
          LOG(INFO) << "send response to pid=" << pid 
            << " for filename=" << iocb->filename;
          auto ret = edgeQueue->write(respMsg);
          assert(ret == 0);
        } else {
          LOG(ERROR) << "not found edge queue for pid=" << pid;
        }

        aio_finish(iocb);
        delete iocb;
      }
      doneCount += iocb_vec.size();
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
  ASDQueue* asdQueue = new ASDQueue("127.0.0.1:21321", maxQueueLen, maxMsgSize);

  auto fut = std::async(std::launch::async, iocompletionFunc);

  bool exit = false;
  int count = 0;

  while (!exit) 
  {
    GatewayMsg anyReq;
    auto ret = asdQueue->read(anyReq);
    assert(ret == 0);

    switch (anyReq.opcode_) {
      case Opcode::ADD_EDGE_REQ:
        {
          LOG(INFO) << "got open for =" << anyReq.edgePid_;
          EdgeQueue* newEdge = new EdgeQueue(anyReq.edgePid_);
          edgeCatalog.insert(std::make_pair(anyReq.edgePid_, newEdge));
          break;
        }
      case Opcode::READ_REQ:
        {
          const int pid = (pid_t)anyReq.edgePid_;
          auto edgeIter = edgeCatalog.find(pid);
          
          if (edgeIter != edgeCatalog.end()) {
            LOG(INFO) << "got read for =" << anyReq.filename_;
            auto edgeQueue = edgeIter->second;

            giocb* iocb = edgeQueue->giocb_from_GatewayMsg(anyReq);

            auto aio_ret = aio_read(ctx, iocb);
            if (aio_ret != 0) {
              anyReq.retval_ = -1;
              anyReq.errval_ = EIO;
              auto ret = edgeQueue->write(anyReq);
              assert(ret == 0);
              delete iocb;
            } else  {
              count ++;
              aio_wait_all(ctx);
            }
          } else {
            LOG(ERROR) << " could not find queue for pid=" << pid;
          }
          break;
        }
      case Opcode::DROP_EDGE_REQ:
        {
          LOG(INFO) << "got close for =" << anyReq.edgePid_;
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
