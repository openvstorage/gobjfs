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

//#include <youtils/Assert.h>
//#include <youtils/Catchers.h>

//#include <ObjectRouter.h>
#include <unistd.h>

#include "NetworkXioIOHandler.h"
#include "NetworkXioProtocol.h"
#include "NetworkXioCommon.h"
#include "NetworkXioWorkQueue.h"
using namespace std;
namespace volumedriverfs
{
#define XIO_COMPLETION_DEFAULT_MAX_EVENTS 100
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
    NetworkXioIOHandler::handle_open(NetworkXioRequest *req,
                                     const std::string& dev_name)
    {
        XXEnter();
        int err = 0;
        GLOG_DEBUG("trying to open volume with name " << dev_name  );
        req->op = NetworkXioMsgOpcode::OpenRsp;
        if (handle_) {
            GLOG_ERROR("Dev " << dev_name <<
                       " is already open for this session");
            req->retval = -1;
            req->errval = EIO;
            pack_msg(req);
            return;
        }
        try {
            err = ObjectFS_Open(dev_name.c_str(), O_WRONLY, &handle_);
            if (err != 0) {
                GLOG_ERROR("ObjectFS_Open failed with error " << err << " for device " << dev_name);
                req->retval = -1;
                req->errval = err;
                pack_msg(req);
                XXExit();
                return;
            }
            dev_name_ = dev_name;
            req->retval = 0;
            req->errval = 0;
        } catch (...) {
            GLOG_ERROR("failed to open volume " << dev_name );
            req->retval = -1;
            req->errval = EIO;
        }
        err = ObjectFS_GetEventFd(handle_, &efd);
        if (err != 0) {
            GLOG_ERROR("ObjectFS_GetEventFd failed with error " << err);
            ObjectFS_Close(&handle_);
            efd = -1;
            req->retval = -1;
            req->errval = EIO;
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
        gObjectMetadata_t *pxioCompObjMeta = NULL;
        gRdStatus *pxioCompRdStatus = NULL;
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
            case NetworkXioMsgOpcode::WriteRsp: {
                    pxioCompObjMeta = (gObjectMetadata_t *)pXioReq->private_data;
                    if (iostatus.errorCode == 0) {
                        assert (pxioCompObjMeta != NULL);
                        GLOG_DEBUG("Write completion for ObjectID: " << pxioCompObjMeta->objectId
                                   << " Written to:"
                                   << " Container: " << pxioCompObjMeta->containerId
                                   << " object offset: " << pxioCompObjMeta->objOffset
                                   << " Segment: " << pxioCompObjMeta->segmentId);
                        // TBD: VEERAL: Now update rocksdb metastore and then call bottom half handler
                        pXioReq->retval = pXioReq->size;
                        pXioReq->errval = iostatus.errorCode;

                    } else {
                        GLOG_ERROR("Write completion error " << iostatus.errorCode << " For completion ID " << iostatus.completionId );
                        // - Just execute bottom half handler.
                        pXioReq->retval = -1;
                        pXioReq->errval = iostatus.errorCode;
                    }
                    pWorkQueue = reinterpret_cast<NetworkXioWorkQueue*> (pXioReq->req_wq);
                    pWorkQueue->worker_bottom_half(pWorkQueue, pXioReq);
                    delete pxioCompObjMeta;
                }
                break;
            case NetworkXioMsgOpcode::ReadRsp: {
                    pxioCompRdStatus = (gRdStatus *)pXioReq->private_data;
                    if (iostatus.errorCode == 0) {
                        GLOG_DEBUG(" Read completed with completion ID" << pxioCompRdStatus->completionId);
                    } else {
                        GLOG_ERROR("Read completion error " << iostatus.errorCode << " For completion ID " << iostatus.completionId );
                    }
                    pWorkQueue = reinterpret_cast<NetworkXioWorkQueue*> (pXioReq->req_wq);
                    pWorkQueue->worker_bottom_half(pWorkQueue, pXioReq);
                    delete pxioCompRdStatus;
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
        if (!handle_) {
            GLOG_ERROR("Device handle null for device " << dev_name_ );
            req->retval = -1;
            req->errval = EIO;
        }
        ObjectFS_Close(&handle_);
        req->retval = 0;
        req->errval = 0;
        done:
        pack_msg(req);
        XXExit();
    }

    int32_t
    NetworkXioIOHandler::ReadIOGetReadObjectInfoFromDB(uint64_t objid_,
                                                       gRdObject_t *pRdObject){
        int errStatus = 0;
        XXEnter();
        if (!pRdObject) {
            GLOG_ERROR("NULL Argument ");
            return -EINVAL;
        }
        // Make RocksDB call to get containerID, OffsetInContainer for given objID.
        pRdObject->containerId = 12; // TODO: Dummy: Veeral
        pRdObject->objOffset = 0; // TODO: Dummy: Veeral

        done:
        XXExit();
        return errStatus;
    }




    void
    NetworkXioIOHandler::handle_read(NetworkXioRequest *req,
                                     uint64_t gobjid_,
                                     size_t size,
                                     uint64_t offset)
    {
        XXEnter();
        int ret = 0;
        req->op = NetworkXioMsgOpcode::ReadRsp;
        req->req_wq = (void *)this->wq_.get();
        GLOG_DEBUG("Received read request for object " << gobjid_ << " at offset " << offset << " for size " << size);
        if (!handle_ ) {
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
            gRdObject_t RdObject_;
            gRdStatus_t *pReadStatus = new gRdStatus_t;
            bool eof = false;
            //req->retval =  pread(handle_, static_cast<char*>(req->data),req->size,req->offset);
            memset(static_cast<char*>(req->data), 0, req->size);
            ret = ReadIOGetReadObjectInfoFromDB(gobjid_, &RdObject_);
            if (ret != 0) {
                GLOG_ERROR("GetReadObjectInfo failed with error " << ret);
                req->retval = -1;
                req->errval = EIO;
                XXDone();
            }
            GLOG_DEBUG("----- The WQ pointer is " << req->req_wq);
            RdObject_.completionId = (gCompletionID)req;
            RdObject_.subObjOffset = req->offset;
            ret = ObjectFS_Get(handle_, &RdObject_, pReadStatus, 1);
            if (ret != 0) {
                if (pReadStatus) {
                    delete pReadStatus;
                    pReadStatus = NULL;
                }
                GLOG_ERROR("ObjectFS_Get failed with error " << ret);
                req->retval = -1;
                req->errval = EIO;
                XXDone();
            }

            GLOG_DEBUG("Do object read for object " << gobjid_ << " at offset " << req->offset);
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
    NetworkXioIOHandler::handle_write(NetworkXioRequest *req,
                                      uint64_t gobjid_,
                                      size_t size,
                                      uint64_t offset)
    {
        XXEnter();
        int err = 0;
        bool sync = false;
        req->op = NetworkXioMsgOpcode::WriteRsp;
        req->req_wq = (void *)this->wq_.get();
        GLOG_DEBUG("Received Write request for object " << gobjid_ << " at offset " << offset << " for size " << size);
        if (!handle_) {
            GLOG_ERROR("Handle is NULL. Trying to write to device which is not opened. Straight Reject! ");
            req->retval = -1;
            req->errval = EIO;
            XXDone();
        }

        if (req->data_len < size) {
            GLOG_ERROR("data buffer size is smaller than the requested write size"
                       " for volume " << dev_name_);
            req->retval = -1;
            req->errval = EIO;
            XXDone();
        }

        req->size = size;
        req->offset = offset;

        try {
            gWrObject wrObject;
            gObjectMetadata_t   *pObjMetadata = new gObjectMetadata_t; // To be freed by the completion thread handler.
            wrObject.buf = static_cast<char*>(req->data);

            req->private_data = pObjMetadata;
            
            wrObject.completionId = (gCompletionID) req;
            wrObject.len = req->size;
            wrObject.objectId = gobjid_;
            GLOG_DEBUG("----- The WQ pointer is " << req->req_wq << " completion id is " << (void *)wrObject.completionId << "operation is " << (int)req->op);

            // The completion thread handler will enqueue the response to finished queue.
            // Refer to bottom half logic in worker_routine() 
            // wq->lock_ts(); wq->finished.push(req);wq->unlock_ts(); 
            err = ObjectFS_Put(handle_, &wrObject, pObjMetadata);
            if (err != 0) {
                if (pObjMetadata) {
                    delete pObjMetadata;
                    pObjMetadata = NULL;
                }
                GLOG_ERROR("ObjectFS_Put() failed with error " << err);
                req->errval = err;
                req->retval = -1;
                XXDone();
            }
            /*fs_.write(*handle_,
                      req->size,
                      static_cast<char*>(req->data),
                      req->offset,
                      sync);*/
            //req->retval = pwrite(handle_, static_cast<char*>(req->data),req->size, req->offset);
            GLOG_DEBUG("Do object write for object " << gobjid_ << " at offset " << req->offset << " of size " << req->size );
            req->retval = 0;
            req->errval = 0;
        }
        /*CATCH_STD_ALL_EWHAT({
           GLOG_ERROR("write I/O error: " << EWHAT);
           req->retval = -1;
           req->errval = EIO;
        });*/
        catch (...) {
            GLOG_ERROR("failed to write " );
            req->retval = -1;
            req->errval = EIO;
        }
        done:
        //if (req->retval < 0) {
         //   GLOG_ERROR("Its an error so packing now ");
        pack_msg(req);
        //}
        GLOG_DEBUG(" Operation is " << (int)req->op );
        XXExit();
    }

    void
    NetworkXioIOHandler::handle_flush(NetworkXioRequest *req)
    {
        XXEnter();
        req->op = NetworkXioMsgOpcode::FlushRsp;
        if (handle_ == NULL) {
            req->retval = -1;
            req->errval = EIO;
            pack_msg(req);
            return;
        }

        GLOG_TRACE("Flushing");
        try {
            //fs_.fsync(*handle_, false);
            //fsync(handle_);
            req->retval = 0;
            req->errval = 0;
        }
        /*CATCH_STD_ALL_EWHAT({
           GLOG_ERROR("flush I/O error: " << EWHAT);
           req->retval = -1;
           req->errval = EIO;
        });*/
        catch (...) {
            GLOG_ERROR("failed to write " );
            req->retval = -1;
            req->errval = EIO;
        }
        pack_msg(req);
        XXExit();
    }

    void
    NetworkXioIOHandler::handle_create_volume(NetworkXioRequest *req,
                                              const std::string& dev_name,
                                              size_t size)
    {
        //VERIFY(not handle_);
        XXEnter();
        assert(!handle_);
        req->op = NetworkXioMsgOpcode::CreateVolumeRsp;
        try {
            //handle_ = creat(volume_path.c_str(), S_IRUSR | S_IWUSR | S_IXUSR);
            if (!handle_) {
                GLOG_ERROR("Problem creating volume");
                assert(0);
                req->retval = -1;
                req->errval = errno;
            } else {
                req->retval = 0;
                req->errval = 0;
                ObjectFS_Close(&handle_);
            }
        } catch (...) {
            GLOG_ERROR("failed to create volume " );
            req->retval = -1;
            req->errval = EIO;
        }
        pack_msg(req);
        XXExit();
    }

    void
    NetworkXioIOHandler::handle_remove_volume(NetworkXioRequest *req,
                                              const std::string& dev_name)
    {
        assert(handle_ != NULL);
        req->op = NetworkXioMsgOpcode::RemoveVolumeRsp;

        const std::string root_("/");
        const std::string dot_(".");
        /*const FrontendPath volume_path(root_ + dev_name + dot_ +
                                       fs_.vdisk_format().name());*/
        const string volume_path(root_ + dev_name);
        try {
            //fs_.unlink(volume_path);
            unlink(volume_path.c_str());
        }
        /*catch (const HierarchicalArakoon::DoesNotExistException&)
        {
            req->retval = -1;
            req->errval = ENOENT;
        }
        CATCH_STD_ALL_EWHAT({
            GLOG_ERROR("Problem removing volume: " << EWHAT);
            req->retval = -1;
            req->errval = EIO;
        });*/
        catch (...) {
            GLOG_ERROR("failed to remove volume " );
            req->retval = -1;
            req->errval = EIO;
        }
        pack_msg(req);
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
                handle_open(req,
                            i_msg.device_name() );
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
                            i_msg.gobjid_,
                            i_msg.size(),
                            i_msg.offset());
                if (req->retval < 0) {
                    pWorkQueue = reinterpret_cast<NetworkXioWorkQueue*> (req->req_wq);
                    pWorkQueue->worker_bottom_half(pWorkQueue, req);
                }
                break;
            }
        case NetworkXioMsgOpcode::WriteReq:
            {
                GLOG_DEBUG(" Command WriteReq");
                if (inents >= 1) {
                    req->data = isglist[0].iov_base;
                    req->data_len = isglist[0].iov_len;
                    GLOG_DEBUG("Request is " << (int)req->op);
                    handle_write(req, 
                                 i_msg.gobjid_,
                                 i_msg.size(), 
                                 i_msg.offset());
                } else {
                    GLOG_ERROR("inents is smaller than 1, cannot proceed with write I/O");
                    req->op = NetworkXioMsgOpcode::WriteRsp;
                    req->retval = -1;
                    req->errval = EIO;
                }
                if (req->retval < 0) {
                    GLOG_DEBUG(" --- WQ pointer is --- " << req->req_wq);
                    pWorkQueue = reinterpret_cast<NetworkXioWorkQueue*> (req->req_wq);
                    pWorkQueue->worker_bottom_half(pWorkQueue, req);
                }
                GLOG_DEBUG("Write success ");
                break;
            }

        case NetworkXioMsgOpcode::FlushReq:
            {
                handle_flush(req);
                break;
            }
        case NetworkXioMsgOpcode::CreateVolumeReq:
            {
                GLOG_DEBUG(" Command CreateVolumeReq");
                handle_create_volume(req,
                                     i_msg.device_name(),
                                     i_msg.size());
                break;
            }
        case NetworkXioMsgOpcode::RemoveVolumeReq:
            {
                GLOG_DEBUG(" Command RemoveVolumeReq");
                handle_remove_volume(req,
                                     i_msg.device_name());
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

} //namespace volumedriverfs
