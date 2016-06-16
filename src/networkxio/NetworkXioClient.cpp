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

#include <thread>
#include <future>
#include <functional>
#include <atomic>
#include <system_error>

#include <sstream>

#include "NetworkXioClient.h"
#include <gobjfs_client.h>
#include "common.h"

#define POLLING_TIME_USEC   20

std::atomic<int> xio_init_refcnt =  ATOMIC_VAR_INIT(0);

static inline void
xrefcnt_init()
{
    if (xio_init_refcnt.fetch_add(1, std::memory_order_relaxed) == 0)
    {
        xio_init();
    }
}

static inline void
xrefcnt_shutdown()
{
    if (xio_init_refcnt.fetch_sub(1, std::memory_order_relaxed) == 1)
    {
        xio_shutdown();
    }
}



namespace gobjfs { namespace xio {

inline void
_xio_aio_wake_up_suspended_aiocb(ovs_aio_request *request)
{
    XXEnter();
    if (not __sync_bool_compare_and_swap(&request->_on_suspend,
                                         false,
                                         true,
                                         __ATOMIC_RELAXED))
    {
        request->_signaled = true;
        GLOG_DEBUG("waking up the suspended thread");
        request->_cvp->signal();
    }
    XXExit();
}

/* called when response is received by NetworkXioClient */
void
ovs_xio_aio_complete_request(void* opaque, ssize_t retval, int errval)
{
    XXEnter();
    ovs_aio_request *request = reinterpret_cast<ovs_aio_request*>(opaque);
    ovs_completion_t *completion = request->completion;
    request->_errno = errval;
    request->_rv = retval;
    request->_failed = (retval == -1 ? true : false);
    request->_completed = true;
    {
        _xio_aio_wake_up_suspended_aiocb(request);
    }
    if (completion)
    {
        completion->_rv = retval;
        completion->_failed = (retval == -1 ? true : false);
        std::cout << "signalling completion" << std::endl;
        // first invoke the callback, then signal completion
        // caller must free the completion in main loop - not in callback!
        completion->complete_cb(completion, completion->cb_arg);
        ovs_aio_signal_completion(completion);
    }
    XXExit();
}

void
ovs_xio_complete_request_control(void *opaque, ssize_t retval, int errval)
{
    XXEnter();
    ovs_aio_request *request = reinterpret_cast<ovs_aio_request*>(opaque);
    if (request)
    {
        request->_errno = errval;
        request->_rv = retval;
    }
    XXExit();
}

template<class T>
static int
static_on_session_event(xio_session *session,
                        xio_session_event_data *event_data,
                        void *cb_user_context)
{
    T *obj = reinterpret_cast<T*>(cb_user_context);
    if (obj == NULL)
    {
        return -1;
    }
    return obj->on_session_event(session, event_data);
}

template<class T>
static int
static_on_response(xio_session *session,
                   xio_msg *req,
                   int last_in_rxq,
                   void *cb_user_context)
{
    T *obj = reinterpret_cast<T*>(cb_user_context);
    if (obj == NULL)
    {
        return -1;
    }
    return obj->on_response(session, req, last_in_rxq);
}

template<class T>
static int
static_on_msg_error(xio_session *session,
                    xio_status error,
                    xio_msg_direction direction,
                    xio_msg *msg,
                    void *cb_user_context)
{
    T *obj = reinterpret_cast<T*>(cb_user_context);
    if (obj == NULL)
    {
        return -1;
    }
    return obj->on_msg_error(session, error, direction, msg);
}

template<class T>
static void
static_evfd_stop_loop(int fd, int events, void *data)
{
    T *obj = reinterpret_cast<T*>(data);
    if (obj == NULL)
    {
        return;
    }
    obj->evfd_stop_loop(fd, events, data);
}

NetworkXioClient::NetworkXioClient(const std::string& uri, const uint64_t qd)
    : uri_(uri)
    , stopping(false)
    , stopped(false)
    , disconnected(false)
    , disconnecting(false)
    , nr_req_queue(qd)
    , evfd()
{
    XXEnter();
    
    ses_ops.on_session_event = static_on_session_event<NetworkXioClient>;
    ses_ops.on_session_established = NULL;
    ses_ops.on_msg = static_on_response<NetworkXioClient>;
    ses_ops.on_msg_error = static_on_msg_error<NetworkXioClient>;
    ses_ops.on_cancel_request = NULL;
    ses_ops.assign_data_in_buf = NULL;

    memset(&params, 0, sizeof(params));
    memset(&cparams, 0, sizeof(cparams));

    params.type = XIO_SESSION_CLIENT;
    params.ses_ops = &ses_ops;
    params.user_context = this;
    params.uri = uri_.c_str();

    int queue_depth = 2 * nr_req_queue;

    xrefcnt_init();

    std::promise<bool> promise;
    std::future<bool> future(promise.get_future());

    try
    {
        xio_thread_ = std::thread([&](){
                try
                {
                    run(promise);
                }
                catch (...)
                {
                    try
                    {
                        promise.set_exception(std::current_exception());
                    } catch(...){}
                }
        });
    }
    catch (const std::system_error&)
    {
        throw XioClientCreateException("failed to create XIO worker thread");
    }

    try
    {
        future.get();
    }
    catch (const std::exception&)
    {
        xio_thread_.join();
        throw XioClientCreateException("failed to create XIO worker thread");
    }

    mpool = std::shared_ptr<xio_mempool>(
                    xio_mempool_create(-1,
                                       XIO_MEMPOOL_FLAG_REGULAR_PAGES_ALLOC),
                    xio_mempool_destroy);
    if (mpool == nullptr)
    {
        shutdown();
        throw XioClientCreateException("failed to create XIO memory pool");
    }
    (void) xio_mempool_add_slab(mpool.get(),
                                4096,
                                0,
                                queue_depth,
                                32,
                                0);
    (void) xio_mempool_add_slab(mpool.get(),
                                32768,
                                0,
                                32,
                                32,
                                0);
    (void) xio_mempool_add_slab(mpool.get(),
                                131072,
                                0,
                                8,
                                32,
                                0);
}

void
NetworkXioClient::run(std::promise<bool>& promise)
{
    int xopt = 0;
    xio_set_opt(NULL,
                XIO_OPTLEVEL_ACCELIO,
                XIO_OPTNAME_ENABLE_FLOW_CONTROL,
                &xopt, sizeof(int));

    xopt = 2 * nr_req_queue;
    xio_set_opt(NULL,
                XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_SND_QUEUE_DEPTH_MSGS,
                &xopt, sizeof(int));

    xio_set_opt(NULL,
                XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_RCV_QUEUE_DEPTH_MSGS,
                &xopt, sizeof(int));

    try
    {
        ctx = std::shared_ptr<xio_context>(
                            xio_context_create(NULL, POLLING_TIME_USEC, -1),
                            xio_destroy_ctx_shutdown);
    }
    catch (const std::bad_alloc&)
    {
      xrefcnt_shutdown();
      throw;
    }

    if (ctx == nullptr)
    {
      throw XioClientCreateException("failed to create XIO context");
    }

    if(xio_context_add_ev_handler(ctx.get(),
                                  evfd,
                                  XIO_POLLIN,
                                  static_evfd_stop_loop<NetworkXioClient>,
                                  this))
    {
//        throw FailedRegisterEventHandler("failed to register event handler");
    }

    session = std::shared_ptr<xio_session>(xio_session_create(&params),
                                           xio_session_destroy);
    if(session == nullptr)
    {
       xio_context_del_ev_handler(ctx.get(), evfd);
       throw XioClientCreateException("failed to create XIO client");
    }

    cparams.session = session.get();
    cparams.ctx = ctx.get();
    cparams.conn_user_context = this;

    conn = xio_connect(&cparams);
    if (conn == nullptr)
    {
      xio_context_del_ev_handler(ctx.get(), evfd);
      throw XioClientCreateException("failed to connect");
    }

    auto fp = std::bind(&NetworkXioClient::xio_run_loop_worker,
                                        this,
                                        std::placeholders::_1);
                    pthread_setname_np(pthread_self(), "xio_run_loop_worker");
    promise.set_value(true);
                    fp(this);
}

void
NetworkXioClient::shutdown()
{
    XXEnter();
    if (not stopped)
    {
      stopping = true;
      xio_context_del_ev_handler(ctx.get(), evfd);
      xio_context_stop_loop(ctx.get());
      xio_thread_.join();
      while (not is_queue_empty())
      {
        xio_msg_s *req = pop_request();
        delete req;
      }
      stopped = true;
    }
}

NetworkXioClient::~NetworkXioClient()
{
    shutdown();
}

int
NetworkXioClient::allocate(xio_reg_mem* mem,
                           const uint64_t size)
{
    return xio_mempool_alloc(mpool.get(), size, mem);
}

void
NetworkXioClient::deallocate(xio_reg_mem *mem)
{
    xio_mempool_free(mem);
}

void
NetworkXioClient::xio_destroy_ctx_shutdown(xio_context *ctx)
{
    xio_context_destroy(ctx);
    xrefcnt_shutdown();
    XXExit();
}

bool
NetworkXioClient::is_queue_empty()
{
    //boost::lock_guard<decltype(inflight_lock)> lock_(inflight_lock);
    //return inflight_reqs.empty();
    XXEnter();
    lock_ts();
    bool isEmpty = inflight_reqs.empty();
    unlock_ts();
    XXExit();
    return isEmpty;
}

NetworkXioClient::xio_msg_s*
NetworkXioClient::pop_request()
{
    XXEnter();
    //boost::lock_guard<decltype(inflight_lock)> lock_(inflight_lock);
    lock_ts();
    xio_msg_s *req = inflight_reqs.front();
    inflight_reqs.pop();
    unlock_ts();
    XXExit();
    return req;
}

void
NetworkXioClient::push_request(xio_msg_s *req)
{
    //boost::lock_guard<decltype(inflight_lock)> lock_(inflight_lock);
    lock_ts();
    inflight_reqs.push(req);
    unlock_ts();
}

void
NetworkXioClient::xstop_loop()
{
    evfd.writefd();
}

void
NetworkXioClient::xio_run_loop_worker(void *arg)
{
    XXEnter();
    NetworkXioClient *cli = reinterpret_cast<NetworkXioClient*>(arg);
    while (not stopping)
    {
        int ret = xio_context_run_loop(cli->ctx.get(), XIO_INFINITE);
        assert(ret == 0);
        while (not cli->is_queue_empty())
        {
            xio_msg_s *req = cli->pop_request();
            int r = xio_send_request(cli->conn, &req->xreq);
            if (r < 0)
            {
                req_queue_release();
                ovs_xio_aio_complete_request(const_cast<void*>(req->opaque),
                                                 -1,
                                                 EIO);
                delete req;
            }
        }
    }

    xio_disconnect(cli->conn);
    if (not disconnected)
    {
        disconnecting = true;
        xio_context_run_loop(cli->ctx.get(), XIO_INFINITE);
    }
    else
    {
        xio_connection_destroy(cli->conn);
    }
    XXExit();
    return;
}

void
NetworkXioClient::evfd_stop_loop(int fd, int /*events*/, void * /*data*/)
{
    XXEnter();
    evfd.readfd();
    xio_context_stop_loop(ctx.get());
    XXExit();
}

int
NetworkXioClient::on_msg_error(xio_session *session __attribute__((unused)),
                               xio_status error __attribute__((unused)),
                               xio_msg_direction direction,
                               xio_msg *msg)
{
    NetworkXioMsg imsg;
    xio_msg_s *xio_msg;
    if (direction == XIO_MSG_DIRECTION_OUT)
    {
        try
        {
            imsg.unpack_msg(static_cast<const char*>(msg->out.header.iov_base),
                             msg->out.header.iov_len);
        }
        catch (...)
        {
            //cnanakos: client logging?
            return 0;
        }
    }
    else /* XIO_MSG_DIRECTION_IN */
    {
        try
        {
            imsg.unpack_msg(static_cast<const char*>(msg->in.header.iov_base),
                            msg->in.header.iov_len);
        }
        catch (...)
        {
            xio_release_response(msg);
            return 0;
        }
        msg->in.header.iov_base = NULL;
        msg->in.header.iov_len = 0;
        vmsg_sglist_set_nents(&msg->in, 0);
        xio_release_response(msg);
    }
    xio_msg = reinterpret_cast<xio_msg_s*>(imsg.opaque());
        ovs_xio_aio_complete_request(const_cast<void*>(xio_msg->opaque),
                                     -1,
                                     EIO);
    req_queue_release();
        delete xio_msg;
    return 0;
}

int
NetworkXioClient::on_session_event(xio_session *session __attribute__((unused)),
                                   xio_session_event_data *event_data)
{
    XXEnter();
    switch (event_data->event)
    {
    case XIO_SESSION_CONNECTION_TEARDOWN_EVENT:
        if (disconnecting)
        {
        xio_connection_destroy(event_data->conn);
        }
        disconnected = true;
        break;
    case XIO_SESSION_TEARDOWN_EVENT:
        xio_context_stop_loop(ctx.get());
        break;
    default:
        break;
    };
    XXExit();
    return 0;
}

void
NetworkXioClient::req_queue_wait_until(xio_msg_s *xmsg)
{
    using namespace std::chrono_literals;
    std::unique_lock<std::mutex> l_(req_queue_lock);
    if (--nr_req_queue <= 0)
    {
        //TODO("export cv timeout")
        if (not req_queue_cond.wait_until(l_,
                                          std::chrono::steady_clock::now() +
                                          60s,
                                          [&]{return nr_req_queue >= 0;}))
        {
           delete xmsg;
           throw XioClientQueueIsBusyException("request queue is busy");
        }
    }
}

void
NetworkXioClient::req_queue_release()
{
    XXEnter();
   // std::lock_guard<std::mutex> l_(req_queue_lock);
    lock_ts();
    nr_req_queue++;
    req_queue_cond.notify_one();
    unlock_ts();
    XXExit();
}

void
NetworkXioClient::xio_send_open_request(const void *opaque)
{
    XXEnter();
    xio_msg_s *xmsg = new xio_msg_s;
    xmsg->opaque = opaque;
    xmsg->msg.opcode(NetworkXioMsgOpcode::OpenReq);
    xmsg->msg.opaque((uintptr_t)xmsg);

    //xmsg->msg.volume_name(volname);

    xio_msg_prepare(xmsg);
    req_queue_wait_until(xmsg);
    push_request(xmsg);
    xstop_loop();
    XXExit();
}

void
NetworkXioClient::xio_send_read_request(const std::string& filename,
                                        void *buf,
                                        const uint64_t size_in_bytes,
                                        const uint64_t offset_in_bytes,
                                        const void *opaque)
{
    XXEnter();
    xio_msg_s *xmsg = new xio_msg_s;
    xmsg->opaque = opaque;
    xmsg->msg.opcode(NetworkXioMsgOpcode::ReadReq);
    xmsg->msg.opaque((uintptr_t)xmsg);
    xmsg->msg.size(size_in_bytes);
    xmsg->msg.offset(offset_in_bytes);
    xmsg->msg.filename_ = filename;

    xio_msg_prepare(xmsg);

    vmsg_sglist_set_nents(&xmsg->xreq.in, 1);
    xmsg->xreq.in.data_iov.sglist[0].iov_base = buf;
    xmsg->xreq.in.data_iov.sglist[0].iov_len = size_in_bytes;
    req_queue_wait_until(xmsg);
    push_request(xmsg);
    xstop_loop();
}


void
NetworkXioClient::xio_send_close_request(const void *opaque)
{
    XXEnter();
    xio_msg_s *xmsg = new xio_msg_s;
    xmsg->opaque = opaque;
    xmsg->msg.opcode(NetworkXioMsgOpcode::CloseReq);
    xmsg->msg.opaque((uintptr_t)xmsg);

    xio_msg_prepare(xmsg);
    req_queue_wait_until(xmsg);
    push_request(xmsg);
    xstop_loop();
    XXExit();
}

int
NetworkXioClient::on_response(xio_session *session __attribute__((unused)),
                              xio_msg *reply,
                              int last_in_rxq __attribute__((unused)))
{
    XXEnter();
    NetworkXioMsg imsg;
    try
    {
        imsg.unpack_msg(static_cast<const char*>(reply->in.header.iov_base),
                         reply->in.header.iov_len);
    }
    catch (...)
    {
        //cnanakos: logging
        return 0;
    }
    xio_msg_s *xio_msg = reinterpret_cast<xio_msg_s*>(imsg.opaque());

    if (imsg.retval() == -1) {
        GLOG_ERROR("\n Alert !!imsg has error in retval\n");
    }
    ovs_xio_aio_complete_request(const_cast<void*>(xio_msg->opaque),
                                 imsg.retval(),
                                 imsg.errval());

    reply->in.header.iov_base = NULL;
    reply->in.header.iov_len = 0;
    vmsg_sglist_set_nents(&reply->in, 0);
    xio_release_response(reply);
    req_queue_release();
    delete xio_msg;
    XXExit();
    return 0;
}

int
NetworkXioClient::on_msg_error_control(xio_session *session ATTRIBUTE_UNUSED,
                                       xio_status error ATTRIBUTE_UNUSED,
                                       xio_msg_direction direction,
                                       xio_msg *msg,
                                       void *cb_user_context ATTRIBUTE_UNUSED)
{
    XXEnter();
    NetworkXioMsg imsg;
    xio_msg_s *xio_msg;

    session_data *sdata = static_cast<session_data*>(cb_user_context);
    xio_context *ctx = sdata->ctx;
    if (direction == XIO_MSG_DIRECTION_IN)
    {
        try
        {
            imsg.unpack_msg(static_cast<const char*>(msg->in.header.iov_base),
                            msg->in.header.iov_len);
        }
        catch (...)
        {
        xio_release_response(msg);
            return 0;
        }
        msg->in.header.iov_base = NULL;
        msg->in.header.iov_len = 0;
        vmsg_sglist_set_nents(&msg->in, 0);
        xio_release_response(msg);
    }
    else /* XIO_MSG_DIRECTION_OUT */
    {
        try
        {
            imsg.unpack_msg(static_cast<const char*>(msg->out.header.iov_base),
                             msg->out.header.iov_len);
        }
        catch (...)
        {
            //cnanakos: client logging?
            return 0;
        }
    }
    xio_msg = reinterpret_cast<xio_msg_s*>(imsg.opaque());
        ovs_xio_complete_request_control(const_cast<void*>(xio_msg->opaque),
                                         -1,
                                         EIO);
    xio_context_stop_loop(ctx);
    XXExit();
    return 0;
}

int
NetworkXioClient::on_msg_control(xio_session *session ATTRIBUTE_UNUSED,
                                 xio_msg *reply,
                                 int last_in_rxq ATTRIBUTE_UNUSED,
                                 void *cb_user_context)
{
    XXEnter();
    session_data *sdata = static_cast<session_data*>(cb_user_context);
    xio_context *ctx = sdata->ctx;
    NetworkXioMsg imsg;
    try
    {
        imsg.unpack_msg(static_cast<const char*>(reply->in.header.iov_base),
                         reply->in.header.iov_len);
    }
    catch (...)
    {
        //cnanakos: logging
        return 0;
    }
    xio_ctl_s *xctl = reinterpret_cast<xio_ctl_s*>(imsg.opaque());
    ovs_xio_complete_request_control(const_cast<void*>(xctl->xmsg.opaque),
                                     imsg.retval(),
                                     imsg.errval());

    
    reply->in.header.iov_base = NULL;
    reply->in.header.iov_len = 0;
    vmsg_sglist_set_nents(&reply->in, 0);
    xio_release_response(reply);
    xio_context_stop_loop(ctx);
    XXExit();
    return 0;
}

int
NetworkXioClient::on_session_event_control(xio_session *session,
                                           xio_session_event_data *event_data,
                                           void *cb_user_context)
{
    XXEnter();
    session_data *sdata = static_cast<session_data*>(cb_user_context);
    xio_context *ctx = sdata->ctx;
    switch (event_data->event)
    {
    case XIO_SESSION_CONNECTION_TEARDOWN_EVENT:
        GLOG_DEBUG("Sending XIO_SESSION_CONNECTION_TEARDOWN_EVENT");
        if (sdata->disconnecting)
        {
        xio_connection_destroy(event_data->conn);
        }
        sdata->disconnected = true;
        xio_context_stop_loop(ctx);
        break;
    case XIO_SESSION_TEARDOWN_EVENT:
        GLOG_DEBUG("Sending XIO_SESSION_TEARDOWN_EVENT");
        xio_session_destroy(session);
        xio_context_stop_loop(ctx);
        break;
    default:
        break;
    }
    XXExit();
    return 0;
}

xio_connection*
NetworkXioClient::create_connection_control(session_data *sdata,
                                            const std::string& uri)
{
    XXEnter();
    xio_connection *conn;
    xio_session *session;
    xio_session_params params;
    xio_connection_params cparams;

    xio_session_ops s_ops;
    s_ops.on_session_event = on_session_event_control;
    s_ops.on_session_established = NULL;
    s_ops.on_msg = on_msg_control;
    s_ops.on_msg_error = on_msg_error_control;
    s_ops.assign_data_in_buf = NULL;

    memset(&params, 0, sizeof(params));
    params.type = XIO_SESSION_CLIENT;
    params.ses_ops = &s_ops;
    params.uri = uri.c_str();
    params.user_context = sdata;

    session = xio_session_create(&params);
    if (not session)
    {
        XXExit();
        return nullptr;
    }
    memset(&cparams, 0, sizeof(cparams));
    cparams.session = session;
    cparams.ctx = sdata->ctx;
    cparams.conn_user_context = sdata;

    conn = xio_connect(&cparams);
    XXExit();
    return conn;
}

void
NetworkXioClient::xio_submit_request(const std::string& uri,
                                     xio_ctl_s *xctl,
                                     void *opaque)
{
    
    XXEnter();
    xrefcnt_init();

    auto ctx = std::shared_ptr<xio_context>(xio_context_create(NULL,
                                                               0,
                                                               -1),
                                            xio_destroy_ctx_shutdown);
    xctl->sdata.ctx = ctx.get();
    xctl->sdata.disconnecting = false;
    xctl->sdata.disconnected = false;
    xio_connection *conn = create_connection_control(&xctl->sdata, uri);
    if (conn == nullptr)
    {
        ovs_xio_complete_request_control(opaque,
                                         -1,
                                         EIO);
        XXExit();
        return;
    }

    int ret = xio_send_request(conn, &xctl->xmsg.xreq);
    if (ret < 0)
    {
        ovs_xio_complete_request_control(opaque,
                                         -1,
                                         EIO);
        goto exit;
    }
    xio_context_run_loop(ctx.get(), XIO_INFINITE);
exit:
    xio_disconnect(conn);
    if (not xctl->sdata.disconnected)
    {
        xctl->sdata.disconnecting = true;
        xio_context_run_loop(ctx.get(), XIO_INFINITE);
    }
    else
    {
        xio_connection_destroy(conn);
    }
    XXExit();
}

void
NetworkXioClient::create_vec_from_buf(xio_ctl_s *xctl,
                                      xio_iovec_ex *sglist,
                                      int vec_size)
{
    uint64_t idx = 0;
    for (int i = 0; i < vec_size; i++)
    {
       assert(sglist);
       xctl->vec->push_back(static_cast<char*>(sglist[0].iov_base) + idx);
       idx += strlen(static_cast<char*>(sglist[0].iov_base)) + 1;
    }
}
void
NetworkXioClient::xio_msg_prepare(xio_msg_s *xmsg)
{
    XXEnter();
    xmsg->s_msg = xmsg->msg.pack_msg();

    memset(static_cast<void*>(&xmsg->xreq), 0, sizeof(xio_msg));

    vmsg_sglist_set_nents(&xmsg->xreq.out, 0);
    xmsg->xreq.out.header.iov_base = (void*)xmsg->s_msg.c_str();
    xmsg->xreq.out.header.iov_len = xmsg->s_msg.length();
    XXExit();
}


}} //namespace gobjfs
