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
#pragma once

#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdint.h>
#include <stdbool.h>
#include <sys/types.h>
#include <vector>
#include <string>
#include <memory> // shared_ptr

namespace gobjfs { namespace xio {

struct gbuffer;

class client_ctx;
typedef std::shared_ptr<client_ctx> client_ctx_ptr;

class client_ctx_attr;
typedef std::shared_ptr<client_ctx_attr> client_ctx_attr_ptr;

struct aio_request;
struct completion;
typedef void (*gcallback)(completion *cb, void *arg);

struct giocb
{
    void *aio_buf;
    off_t aio_offset;
    size_t aio_nbytes;
    aio_request *request_;
};


/*
 * Create context attributes object
 * param attr: Context attributes object
 * return: This function always succeeds returning 0
 * return: Context attributes object on success, or NULL on fail
 */
client_ctx_attr_ptr
ctx_attr_new();

/*
 * Set transport type
 * param attr: Context attributes object
 * param transport: Transport string ("tcp" or "rdma)
 * param host: Host string FQDN or IP address
 * param port: TCP/RDMA port number
 * return: 0 on success, -1 on fail
 */
int
ctx_attr_set_transport(client_ctx_attr_ptr attr,
                           std::string const &transport,
                           std::string const &host,
                           int port);

/*
 * Create Open vStorage context
 * param attr: Context attributes object
 * return: Open vStorage context on success, or NULL on fail
 */
client_ctx_ptr
ctx_new(const client_ctx_attr_ptr attr);

/*
 * Initialize Open vStorage context to talk to server
 * param ctx: Open vStorage context
 * return: Open vStorage context on success, or NULL on fail
 */
int
ctx_init(client_ctx_ptr ctx);

/**
 * Get stats for this connection
 * param ctx : Open vStorage context
 * return : statistics on read requests
 */
std::string
ctx_get_stats(client_ctx_ptr ctx);


/*
 * Check connection status
 * param ctx: Open vStorage context
 * return: True if the client has been disconnected, false otherwise
 */
bool
ctx_is_disconnected(client_ctx_ptr ctx);

/*
 * Allocate buffer from the shared memory segment
 * param ctx: Open vStorage context
 * param size: Buffer size in bytes
 * return: Buffer pointer on success, or NULL on fail
 */
gbuffer*
gbuffer_allocate(client_ctx_ptr ctx,
             size_t size);

/* Retrieve pointer to buffer content
 * param ptr: Pointer to buffer structure
 * return: Buffer pointer on success, or NULL on fail
 */
void*
gbuffer_data(gbuffer *ptr);

/* Retrieve size of buffer
 * param ptr: Pointer to buffer structure
 * return: Size of buffer on success, -1 on fail
 */
size_t
gbuffer_size(gbuffer *ptr);

/*
 * Deallocate previously allocated buffer
 * param ctx: Open vStorage context
 * param shptr: Buffer pointer
 * return: 0 on success, -1 on fail
 */
int
gbuffer_deallocate(client_ctx_ptr ctx,
               gbuffer *ptr);

/*
 * Read from a volume
 * param ctx: Open vStorage context
 * param buf: Shared memory buffer
 * param nbytes: Size to read in bytes
 * param offset: Offset to read in volume
 * return: Number of bytes actually read, -1 on fail
 */
ssize_t
read(client_ctx_ptr ctx,
         const std::string& filename,
         void *buf,
         size_t nbytes,
         off_t offset);


/*
 * Suspend until asynchronous I/O operation or timeout complete
 * param ctx: Open vStorage context
 * param giocb: Pointer to an AIO Control Block structure
 * param timeout: Pointer to a timespec structure
 * return: 0 on success, -1 on fail
 */
int
aio_suspend(client_ctx_ptr ctx,
                giocb *giocb,
                const timespec *timeout);

/*
 * Suspend until asynchronous I/O operation or timeout complete
 * param ctx: Open vStorage context
 * param giocb_vec: Pointer to vector of AIO Control Block structure
 * param timeout: Pointer to a timespec structure
 * return: 0 on success, -1 on fail
 */

int
aio_suspendv(client_ctx_ptr ctx,
                const std::vector<giocb*> &giocbp_vec,
                const timespec *timeout);
/*
 * Retrieve error status of asynchronous I/O operation
 * param ctx: Open vStorage context
 * param giocb: Pointer to an AIO Control Block structure
 * return: 0 on success, -1 on fail
 */
int
aio_error(client_ctx_ptr ctx,
              giocb *giocbp);

/*
 * Retrieve return status of asynchronous I/O operation
 * param ctx: Open vStorage context
 * param giocb: Pointer to an AIO Control Block structure
 * return: Number of bytes returned based on the operation, -1 on fail
 */
ssize_t
aio_return(client_ctx_ptr ctx,
               giocb *giocbp);

/*
 * Cancel an oustanding asynchronous I/O operation
 * param ctx: Open vStorage context
 * param giocb: Pointer to an AIO Control Block structure
 * return: 0 on success, -1 on fail
 */
int
aio_cancel(client_ctx_ptr ctx,
               giocb *giocbp);

/*
 * Finish an asynchronous I/O operation
 * param ctx: Open vStorage context
 * param giocb: Pointer to an AIO Control Block structure
 * return: 0 on success, -1 on fail
 */
int
aio_finish(client_ctx_ptr ctx,
               giocb* giocbp);

/*
 * Asynchronous read from a volume
 * param ctx: Open vStorage context
 * param filename: filenames on which to read
 * param giocb: Pointer to an AIO Control Block structure
 * return: 0 on success, -1 on fail
 */
int
aio_read(client_ctx_ptr ctx,
             const std::string  &filename,
             giocb *giocbp);

/*
 * Asynchronous readv from a volume
 * param ctx: Open vStorage context
 * param filename_vec: Pointer to vector of filenames to read
 * param giocb_vec: Pointer to vector of AIO Control Block structure
 *   this vector and filename_vec must be same size
 * return: 0 on success, -1 on fail
 */
int
aio_readv(client_ctx_ptr ctx,
             const std::vector<std::string> &filename_vec,
             const std::vector<giocb*> &giocbp_vec);

/*
 * Asynchronous read from a volume with completion
 * param ctx: Open vStorage context
 * param giocb: Pointer to an AIO Control Block structure
 * param completion: Pointer to a completion structure
 * return: 0 on success, -1 on fail
 */
int
aio_readcb(client_ctx_ptr ctx,
               const std::string &filename,
               giocb *giocbp,
               completion *completion);

/*
 * Create a new completion
 * param complete_cb: Pointer to an gcallback structure
 * param arg: Pointer to an argument passed to complete_cb
 * return: Completion pointer on success, or NULL on fail
 */
completion*
aio_create_completion(gcallback complete_cb,
                          void *arg);

/*
 * Retrieve return status of a completion
 * param completion: Pointer to a completion structure
 * return: Number of bytes returned based on the operation and the completion,
 * -1 on fail
 */
ssize_t
aio_return_completion(completion *completion);

/*
 * Suspend until completion or timeout complete
 * param completion: Pointer to completion structure
 * param timeout: Pointer to a timespec structure
 * return: 0 on success, -1 on fail
 */
int
aio_wait_completion(completion *completion,
                        const timespec *timeout);

/*
 * Signal a suspended completion
 * param completion: Pointer to completion structure
 * return: 0 on success, -1 on fail
 */
int
aio_signal_completion(completion *completion);

/*
 * Release completion
 * param completion: Pointer to completion structure
 * return: 0 on success, -1 on fail
 */
int
aio_release_completion(completion *completion);


}}
