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

typedef struct ovs_buffer ovs_buffer_t;

class ovs_context_t;
typedef std::shared_ptr<ovs_context_t> ovs_ctx_ptr;

class ovs_context_attr_t;
typedef std::shared_ptr<ovs_context_attr_t> ovs_ctx_attr_ptr;

typedef struct ovs_aio_request ovs_aio_request;
typedef struct ovs_completion ovs_completion_t;
typedef void (*ovs_callback_t)(ovs_completion_t *cb, void *arg);

struct ovs_aiocb
{
    void *aio_buf;
    off_t aio_offset;
    size_t aio_nbytes;
    ovs_aio_request *request_;
};


/*
 * Create context attributes object
 * param attr: Context attributes object
 * return: This function always succeeds returning 0
 * return: Context attributes object on success, or NULL on fail
 */
ovs_ctx_attr_ptr
ovs_ctx_attr_new();

/*
 * Set transport type
 * param attr: Context attributes object
 * param transport: Transport string ("tcp" or "rdma)
 * param host: Host string FQDN or IP address
 * param port: TCP/RDMA port number
 * return: 0 on success, -1 on fail
 */
int
ovs_ctx_attr_set_transport(ovs_ctx_attr_ptr attr,
                           const char *transport,
                           const char *host,
                           int port);

/*
 * Create Open vStorage context
 * param attr: Context attributes object
 * return: Open vStorage context on success, or NULL on fail
 */
ovs_ctx_ptr
ovs_ctx_new(const ovs_ctx_attr_ptr attr);

/*
 * Initialize Open vStorage context to talk to server
 * param ctx: Open vStorage context
 * return: Open vStorage context on success, or NULL on fail
 */
int
ovs_ctx_init(ovs_ctx_ptr ctx);

/*
 * Allocate buffer from the shared memory segment
 * param ctx: Open vStorage context
 * param size: Buffer size in bytes
 * return: Buffer pointer on success, or NULL on fail
 */
ovs_buffer_t*
ovs_allocate(ovs_ctx_ptr ctx,
             size_t size);

/* Retrieve pointer to buffer content
 * param ptr: Pointer to buffer structure
 * return: Buffer pointer on success, or NULL on fail
 */
void*
ovs_buffer_data(ovs_buffer_t *ptr);

/* Retrieve size of buffer
 * param ptr: Pointer to buffer structure
 * return: Size of buffer on success, -1 on fail
 */
size_t
ovs_buffer_size(ovs_buffer_t *ptr);

/*
 * Deallocate previously allocated buffer
 * param ctx: Open vStorage context
 * param shptr: Buffer pointer
 * return: 0 on success, -1 on fail
 */
int
ovs_deallocate(ovs_ctx_ptr ctx,
               ovs_buffer_t *ptr);

/*
 * Read from a volume
 * param ctx: Open vStorage context
 * param buf: Shared memory buffer
 * param nbytes: Size to read in bytes
 * param offset: Offset to read in volume
 * return: Number of bytes actually read, -1 on fail
 */
ssize_t
ovs_read(ovs_ctx_ptr ctx,
         const std::string& filename,
         void *buf,
         size_t nbytes,
         off_t offset);


/*
 * Suspend until asynchronous I/O operation or timeout complete
 * param ctx: Open vStorage context
 * param ovs_aiocb: Pointer to an AIO Control Block structure
 * param timeout: Pointer to a timespec structure
 * return: 0 on success, -1 on fail
 */
int
ovs_aio_suspend(ovs_ctx_ptr ctx,
                struct ovs_aiocb *ovs_aiocb,
                const struct timespec *timeout);

/*
 * Suspend until asynchronous I/O operation or timeout complete
 * param ctx: Open vStorage context
 * param ovs_aiocb_vec: Pointer to vector of AIO Control Block structure
 * param timeout: Pointer to a timespec structure
 * return: 0 on success, -1 on fail
 */

int
ovs_aio_suspendv(ovs_ctx_ptr ctx,
                const std::vector<ovs_aiocb*> &ovs_aiocbp_vec,
                const struct timespec *timeout);
/*
 * Retrieve error status of asynchronous I/O operation
 * param ctx: Open vStorage context
 * param ovs_aiocb: Pointer to an AIO Control Block structure
 * return: 0 on success, -1 on fail
 */
int
ovs_aio_error(ovs_ctx_ptr ctx,
              struct ovs_aiocb *ovs_aiocbp);

/*
 * Retrieve return status of asynchronous I/O operation
 * param ctx: Open vStorage context
 * param ovs_aiocb: Pointer to an AIO Control Block structure
 * return: Number of bytes returned based on the operation, -1 on fail
 */
ssize_t
ovs_aio_return(ovs_ctx_ptr ctx,
               struct ovs_aiocb *ovs_aiocbp);

/*
 * Cancel an oustanding asynchronous I/O operation
 * param ctx: Open vStorage context
 * param ovs_aiocb: Pointer to an AIO Control Block structure
 * return: 0 on success, -1 on fail
 */
int
ovs_aio_cancel(ovs_ctx_ptr ctx,
               struct ovs_aiocb *ovs_aiocbp);

/*
 * Finish an asynchronous I/O operation
 * param ctx: Open vStorage context
 * param ovs_aiocb: Pointer to an AIO Control Block structure
 * return: 0 on success, -1 on fail
 */
int
ovs_aio_finish(ovs_ctx_ptr ctx,
               struct ovs_aiocb* ovs_aiocbp);

/*
 * Asynchronous read from a volume
 * param ctx: Open vStorage context
 * param filename: filenames on which to read
 * param ovs_aiocb: Pointer to an AIO Control Block structure
 * return: 0 on success, -1 on fail
 */
int
ovs_aio_read(ovs_ctx_ptr ctx,
             const std::string  &filename,
             struct ovs_aiocb *ovs_aiocbp);

/*
 * Asynchronous readv from a volume
 * param ctx: Open vStorage context
 * param filename_vec: Pointer to vector of filenames to read
 * param ovs_aiocb_vec: Pointer to vector of AIO Control Block structure
 *   this vector and filename_vec must be same size
 * return: 0 on success, -1 on fail
 */
int
ovs_aio_readv(ovs_ctx_ptr ctx,
             const std::vector<std::string> &filename_vec,
             const std::vector<ovs_aiocb*> &ovs_aiocbp_vec);

/*
 * Asynchronous read from a volume with completion
 * param ctx: Open vStorage context
 * param ovs_aiocb: Pointer to an AIO Control Block structure
 * param completion: Pointer to a completion structure
 * return: 0 on success, -1 on fail
 */
int
ovs_aio_readcb(ovs_ctx_ptr ctx,
               const std::string &filename,
               struct ovs_aiocb *ovs_aiocbp,
               ovs_completion_t *completion);

/*
 * Create a new completion
 * param complete_cb: Pointer to an ovs_callback_t structure
 * param arg: Pointer to an argument passed to complete_cb
 * return: Completion pointer on success, or NULL on fail
 */
ovs_completion_t*
ovs_aio_create_completion(ovs_callback_t complete_cb,
                          void *arg);

/*
 * Retrieve return status of a completion
 * param completion: Pointer to a completion structure
 * return: Number of bytes returned based on the operation and the completion,
 * -1 on fail
 */
ssize_t
ovs_aio_return_completion(ovs_completion_t *completion);

/*
 * Suspend until completion or timeout complete
 * param completion: Pointer to completion structure
 * param timeout: Pointer to a timespec structure
 * return: 0 on success, -1 on fail
 */
int
ovs_aio_wait_completion(ovs_completion_t *completion,
                        const struct timespec *timeout);

/*
 * Signal a suspended completion
 * param completion: Pointer to completion structure
 * return: 0 on success, -1 on fail
 */
int
ovs_aio_signal_completion(ovs_completion_t *completion);

/*
 * Release completion
 * param completion: Pointer to completion structure
 * return: 0 on success, -1 on fail
 */
int
ovs_aio_release_completion(ovs_completion_t *completion);

