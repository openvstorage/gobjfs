// Copyright 2016 iNuron NV
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include <unistd.h>

#include "NetworkXioIOHandler.h"
#include "NetworkXioProtocol.h"
#include "NetworkXioCommon.h"
#include "NetworkXioWorkQueue.h"
using namespace std;
namespace gobjfs { namespace xio {

static constexpr int XIO_COMPLETION_DEFAULT_MAX_EVENTS = 100;

    static int 
    gxio_completion_handler(int epollfd, int efd);

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

    void
    NetworkXioIOHandler::handle_open(NetworkXioRequest *req)
    {
        XXEnter();
        int err = 0;
        GLOG_DEBUG("trying to open volume ");

        req->op = NetworkXioMsgOpcode::OpenRsp;
        if (serviceHandle_) {
            GLOG_ERROR("Dev is already open for this session");
            req->retval = -1;
            req->errval = EIO;
            pack_msg(req);
            return;
        }
        try {
            serviceHandle_ = IOExecFileServiceInit(configFileName_.c_str());
            if (serviceHandle_ == nullptr) {
                GLOG_ERROR("file service init failed");
                req->retval = -1;
                req->errval = err;
                pack_msg(req);
                XXExit();
                return;
            }
            req->retval = 0;
            req->errval = 0;
        } catch (...) {
            GLOG_ERROR("failed to open volume ");
            req->retval = -1;
            req->errval = EIO;
        }
        eventHandle_ = IOExecEventFdOpen(serviceHandle_);
        if (eventHandle_ == nullptr) {
            GLOG_ERROR("ObjectFS_GetEventFd failed with error " << err);
            //ObjectFS_Close(&handle_);
            req->retval = -1;
            req->errval = EIO;
            // TODO return
        }

        auto efd = IOExecEventFdGetReadFd(eventHandle_);
        if (efd == -1) {
            GLOG_ERROR("GetReadFd failed with ret " << efd);
            req->retval = -1;
            req->errval = EIO;
            // TODO return
        }

        epollfd = epoll_create1(0);
        assert(epollfd >= 0);

        struct epoll_event event;
        event.data.fd = efd;
        event.events = EPOLLIN | EPOLLONESHOT;
        err = epoll_ctl(epollfd, EPOLL_CTL_ADD, efd, &event);
        if (err != 0) {
            GLOG_ERROR("epoll_ctl() failed with error " << errno);
            assert(0);
        }
        pioCompletionThread  = new std::thread(gxio_completion_handler, epollfd, efd);

        pack_msg(req);
        XXExit();
    }

    static int 
    gxio_completion_handler(int epollfd, int efd) {
        int ret = 0;

        NetworkXioWorkQueue* pWorkQueue = NULL;
        unsigned int max = XIO_COMPLETION_DEFAULT_MAX_EVENTS;
        XXEnter();
        epoll_event *events = (struct epoll_event *)calloc (max, sizeof (epoll_event));

        // LEVELTRIGGERED is default
        // Edge triggerd
   while (1) {
        int n = epoll_wait (epollfd, events, max, -1);
        for (int i = 0; i < n ; i++) {
            if (efd != events[i].data.fd) {
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
        struct epoll_event event;
        event.data.fd = efd;
        event.events = EPOLLIN | EPOLLONESHOT;
        int s = epoll_ctl (epollfd, EPOLL_CTL_MOD, efd, &event);
        assert(s == 0);
      }

        free(events);
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
        IOExecFileServiceDestroy(serviceHandle_);
        req->retval = 0;
        req->errval = 0;
        done:
        pack_msg(req);
        XXExit();
    }

    void
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

            IOExecFileHandle fileHandle = IOExecFileOpen(serviceHandle_, filename.c_str(), O_RDWR);

            gIOBatch* batch = gIOBatchAlloc(1);

            gIOExecFragment& frag = batch->array[0];

            frag.offset = offset;
            frag.addr = reinterpret_cast<caddr_t>(req->reg_mem.addr);
            frag.size = size;
            frag.completionId = reinterpret_cast<uint64_t>(req);

            ret = IOExecFileRead(fileHandle, batch, eventHandle_);

            if (ret != 0) {
                GLOG_ERROR("IOExecFileRead failed with error " << ret);
                req->retval = -1;
                req->errval = EIO;
                XXDone();
            }

            GLOG_DEBUG("Do object read for object " << filename << " at offset " << req->offset);
            req->errval = 0;
            req->retval = req->size;
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
        pack_msg(req);
        XXExit();
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
                handle_read(req,
                            i_msg.filename_,
                            i_msg.size(),
                            i_msg.offset());
                if (req->retval < 0) {
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
