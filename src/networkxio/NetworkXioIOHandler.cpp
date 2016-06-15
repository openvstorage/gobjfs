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

#include <unistd.h>

#include "volumedriver.h"
#include "common.h"

#include "NetworkXioIOHandler.h"
#include <sys/epoll.h>
#include "NetworkXioCommon.h"
#include "NetworkXioProtocol.h"
#include "NetworkXioWorkQueue.h"

using namespace std;
namespace gobjfs { namespace xio {

static constexpr int XIO_COMPLETION_DEFAULT_MAX_EVENTS = 100;


    static inline void
    pack_msg(NetworkXioRequest *req)
    {
        XXEnter();
        NetworkXioMsg o_msg(req->op);
        o_msg.retval(req->retval);
        o_msg.errval(req->errval);
        o_msg.opaque(req->opaque);
        req->s_msg= o_msg.pack_msg();
        XXExit();
    }

    NetworkXioIOHandler::NetworkXioIOHandler(const std::string& configFileName,
      NetworkXioWorkQueuePtr wq)
    : configFileName_(configFileName)
    , wq_(wq) 
    {
        try {

          serviceHandle_ = IOExecFileServiceInit(configFileName_.c_str());
          if (serviceHandle_ == nullptr) {
              throw std::bad_alloc();
          }

          eventHandle_ = IOExecEventFdOpen(serviceHandle_);
          if (eventHandle_ == nullptr) {
            GLOG_ERROR("ObjectFS_GetEventFd failed with error ");
          }

          auto efd = IOExecEventFdGetReadFd(eventHandle_);
          if (efd == -1) {
            GLOG_ERROR("GetReadFd failed with ret " << efd);
          }

          epollfd = epoll_create1(0);
          assert(epollfd >= 0);

          struct epoll_event event;
          event.data.fd = efd;
          event.events = EPOLLIN;
          int err = epoll_ctl(epollfd, EPOLL_CTL_ADD, efd, &event);
          if (err != 0) {
            GLOG_ERROR("epoll_ctl() failed with error " << errno);
            assert(0);
          }

          err = ioCompletionThreadShutdown.init(epollfd);
          if (err != 0) {
            GLOG_ERROR("failed to init shutdown notifier " << err);
            assert(0);
          }

          ioCompletionThread  = std::thread(
            std::bind(&NetworkXioIOHandler::gxio_completion_handler, 
              this, 
              epollfd, 
              efd));

        } catch (...) {
            GLOG_ERROR("failed to init handler");
        }
    }

    NetworkXioIOHandler::~NetworkXioIOHandler()
    {
      try {
        int err = ioCompletionThreadShutdown.send();
        if (err != 0) {
          GLOG_ERROR("failed to notify completion thread");
        } else {
          ioCompletionThread.join();     
        }

        ioCompletionThreadShutdown.destroy();

      } catch (const std::exception& e) {
        GLOG_ERROR("failed to join completion thread");
      }

      if (eventHandle_) {
        IOExecEventFdClose(eventHandle_);
        eventHandle_ = nullptr;
      }

      if (epollfd != -1) {
        close(epollfd);
        epollfd = -1;
      }

      if (serviceHandle_ ) {
        IOExecFileServiceDestroy(serviceHandle_);
        serviceHandle_ = nullptr;
      }
    }

    void
    NetworkXioIOHandler::handle_open(NetworkXioRequest *req)
    {
        XXEnter();
        int err = 0;
        GLOG_DEBUG("trying to open volume ");

        req->op = NetworkXioMsgOpcode::OpenRsp;
        req->retval = 0;
        req->errval = 0;

        pack_msg(req);
        XXExit();
    }

    int 
    NetworkXioIOHandler::gxio_completion_handler(int epollfd, int efd) {
      int ret = 0;

      XXEnter();

      NetworkXioWorkQueue* pWorkQueue = NULL;
      const unsigned int max = XIO_COMPLETION_DEFAULT_MAX_EVENTS;
      epoll_event events[max];

      bool mustExit = false;

      while (!mustExit) {

        int n = epoll_wait (epollfd, events, max, -1);

        for (int i = 0; i < n ; i++) {

          if (events[i].data.ptr == &ioCompletionThreadShutdown) {
            uint64_t counter;
            ioCompletionThreadShutdown.recv(counter);
            mustExit = true;
            GLOG_DEBUG("Received shutdown event for ptr=" << (void*)this);
            continue;
          }

          if (efd != events[i].data.fd) {
            GLOG_ERROR("Received event for unknown fd=" << events[i].data.fd);
            continue;
          }

          gIOStatus iostatus;
          int ret = read(efd, &iostatus, sizeof (iostatus));

          if (ret != sizeof(iostatus)) {
            GLOG_ERROR("!!! Partial read .. Cant handle it ... Need to think abt. TBD !! ");
            continue;
          }

          GLOG_DEBUG("Recieved event" << " (completionId: " << (void *)iostatus.completionId << " status: " << iostatus.errorCode);

          NetworkXioRequest *pXioReq = (NetworkXioRequest *)iostatus.completionId;
          assert (pXioReq != nullptr);

          if (iostatus.errorCode == 0 ) {
              pXioReq->retval = 0;
              pXioReq->errval = 0;
          } else {
              pXioReq->retval = -1;
              pXioReq->errval = iostatus.errorCode;
          }


          switch (pXioReq->op) {

          case NetworkXioMsgOpcode::ReadRsp: {
                  if (iostatus.errorCode == 0) {
                      GLOG_DEBUG(" Read completed with completion ID" << iostatus.completionId);
                  } else {
                      GLOG_ERROR("Read completion error " << iostatus.errorCode << " For completion ID " << iostatus.completionId );
                  }
                  pack_msg(pXioReq);
                  pWorkQueue = reinterpret_cast<NetworkXioWorkQueue*> (pXioReq->req_wq);
                  pWorkQueue->worker_bottom_half(pWorkQueue, pXioReq);
              }
              break;

          default:{
                  GLOG_ERROR("Got an event for non rd/wr operation " << (int )pXioReq->op << ".. WTF ! Must fail assert");
                  assert(0);
              }

          }
        }
      }

      return 0;
    }

    void
    NetworkXioIOHandler::handle_close(NetworkXioRequest *req)
    {
        XXEnter();
        req->op = NetworkXioMsgOpcode::CloseRsp;
        if (!serviceHandle_) {
            GLOG_ERROR("Device handle null for device ");
            req->retval = -1;
            req->errval = EIO;
        }
        req->retval = 0;
        req->errval = 0;
        done:
        pack_msg(req);
        XXExit();
    }

    int
    NetworkXioIOHandler::handle_read(NetworkXioRequest *req,
                                     const std::string& filename, 
                                     size_t size,
                                     off_t offset)
    {
        XXEnter();
        int ret = 0;
        req->op = NetworkXioMsgOpcode::ReadRsp;
        req->req_wq = (void *)this->wq_.get();
        GLOG_DEBUG("Received read request for object " << filename << " at offset " << offset << " for size " << size);
        if (!serviceHandle_) {
            req->retval = -1;
            req->errval = EIO;
            XXDone();
        }

        ret = xio_mempool_alloc(req->pClientData->ncd_mpool, size, &req->reg_mem);
        if (ret < 0) {
            ret = xio_mem_alloc(size, &req->reg_mem);
            if (ret < 0) {
                GLOG_ERROR("cannot allocate requested buffer, size: " << size);
                req->retval = -1;
                req->errval = ENOMEM;
                XXDone();
            }
            req->from_pool = false;
        }

        req->data = req->reg_mem.addr;
        req->data_len = size;
        req->size = size;
        req->offset = offset;
        try {
            bool eof = false;

            memset(static_cast<char*>(req->data), 0, req->size);

            if (ret != 0) {
                GLOG_ERROR("GetReadObjectInfo failed with error " << ret);
                req->retval = -1;
                req->errval = EIO;
                XXDone();
            }
            GLOG_DEBUG("----- The WQ pointer is " << req->req_wq);

            gIOBatch* batch = gIOBatchAlloc(1);

            gIOExecFragment& frag = batch->array[0];

            frag.offset = offset;
            frag.addr = reinterpret_cast<caddr_t>(req->reg_mem.addr);
            frag.size = size;
            frag.completionId = reinterpret_cast<uint64_t>(req);

            ret = IOExecFileRead(serviceHandle_, filename.c_str(), batch, eventHandle_);

            if (ret != 0) {
                GLOG_ERROR("IOExecFileRead failed with error " << ret);
                req->retval = -1;
                req->errval = EIO;
                XXDone();
            }

            // CAUTION : only touch "req" after this if ret is non-zero
            // because "req" can get freed by the other thread if the IO completed
            // leading to a "use-after-free" memory error 
            XXDone();
        }
        /*CATCH_STD_ALL_EWHAT({
           GLOG_ERROR("read I/O error: " << EWHAT);
           req->retval = -1;
           req->errval = EIO;
        });*/
        catch (...) {
            GLOG_ERROR("failed to read volume " );
            req->retval = -1;
            req->errval = EIO;
        }
        done:
        if (ret != 0) { 
          pack_msg(req);
        }
        XXExit();
        return ret;
    }

    void
    NetworkXioIOHandler::handle_error(NetworkXioRequest *req, int errval)
    {
        req->op = NetworkXioMsgOpcode::ErrorRsp;
        req->retval = -1;
        req->errval = errval;
        pack_msg(req);
    }

    void
    NetworkXioIOHandler::process_request(NetworkXioRequest *req)
    {
        XXEnter();
        NetworkXioWorkQueue *pWorkQueue = NULL;
        xio_msg *xio_req = req->xio_req;
        xio_iovec_ex *isglist = vmsg_sglist(&xio_req->in);
        int inents = vmsg_sglist_nents(&xio_req->in);

        req->pClientData->ncd_refcnt++;
        NetworkXioMsg i_msg(NetworkXioMsgOpcode::Noop);
        try {
            i_msg.unpack_msg(static_cast<char*>(xio_req->in.header.iov_base),
                             xio_req->in.header.iov_len);
        } catch (...) {
            GLOG_ERROR("cannot unpack message");
            handle_error(req, EBADMSG);
            return;
        }

        req->opaque = i_msg.opaque();
        switch (i_msg.opcode()) {
        case NetworkXioMsgOpcode::OpenReq:
            {
                GLOG_DEBUG(" Command OpenReq");
                handle_open(req);
                break;
            }
        case NetworkXioMsgOpcode::CloseReq:
            {
                GLOG_DEBUG(" Command CloseReq");
                handle_close(req);
                break;
            }
        case NetworkXioMsgOpcode::ReadReq:
            {
                GLOG_DEBUG(" Command ReadReq");
                auto ret = handle_read(req,
                            i_msg.filename_,
                            i_msg.size(),
                            i_msg.offset());
                if (ret != 0) {
                    pWorkQueue = reinterpret_cast<NetworkXioWorkQueue*> (req->req_wq);
                    pWorkQueue->worker_bottom_half(pWorkQueue, req);
                }
                break;
            }
        default:
            XXExit();
            GLOG_ERROR("Unknown command");
            handle_error(req, EIO);
            return;
        }; 
        XXExit();
    }

    void
    NetworkXioIOHandler::handle_request(NetworkXioRequest *req)
    {
        XXEnter();
        req->work.func = std::bind(&NetworkXioIOHandler::process_request,
                                   this,
                                   req);
        wq_->work_schedule(req);
        XXExit();
    }

}} //namespace gobjfs
