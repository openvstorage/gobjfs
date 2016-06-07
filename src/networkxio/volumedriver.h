// This file is dual licensed GPLv2 and Apache 2.0.
// Active license depends on how it is used.
//
// Copyright 2016 iNuron NV
//
// // GPL //
// This file is part of OpenvStorage.
//
// OpenvStorage is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with OpenvStorage. If not, see <http://www.gnu.org/licenses/>.
//
// // Apache 2.0 //
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef __LIB_OVS_VOLUMEDRIVER_H_
#define __LIB_OVS_VOLUMEDRIVER_H_

#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct ovs_buffer ovs_buffer_t;
typedef struct ovs_context_t ovs_ctx_t;
typedef struct ovs_context_attr_t ovs_ctx_attr_t;
typedef struct ovs_snapshot_info ovs_snapshot_info_t;
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

struct ovs_snapshot_info
{
    const char *name;
    uint64_t size;
};

/*
 * Create context attributes object
 * param attr: Context attributes object
 * return: This function always succeeds returning 0
 * return: Context attributes object on success, or NULL on fail
 */
ovs_ctx_attr_t*
ovs_ctx_attr_new();

/*
 * Destroy context attributes object
 * param attr: Context attributes object
 * return: This function always succeeds returning 0
 */
int
ovs_ctx_attr_destroy(ovs_ctx_attr_t *attr);

/*
 * Set transport type
 * param attr: Context attributes object
 * param transport: Transport string ("shm", "tcp", "rdma)
 * param host: Host string (FQDN or ASCII)
 * param port: TCP/RDMA port number
 * return: 0 on success, -1 on fail
 */
int
ovs_ctx_attr_set_transport(ovs_ctx_attr_t *attr,
                           const char *transport,
                           const char *host,
                           int port);

/*
 * Create Open vStorage context
 * param attr: Context attributes object
 * return: Open vStorage context on success, or NULL on fail
 */
ovs_ctx_t*
ovs_ctx_new(const ovs_ctx_attr_t *attr);

/*
 * Initialize Open vStorage context
 * param ctx: Open vStorage context
 * param dev_name: Volume name
 * param oflag: Open flags
 * return: Open vStorage context on success, or NULL on fail
 */
int
ovs_ctx_init(ovs_ctx_t *ctx,
             const char *dev_name,
             int oflag);

/*
 * Destroy Open vStorage context
 * param: Open vStorage context
 * return: 0 on success, -1 on fail
 */
int
ovs_ctx_destroy(ovs_ctx_t *ctx);

/*
 * Create an Open vStorage volume
 * param ctx: Open vStorage context
 * param dev_name: Volume name
 * param size: Size of volume in bytes
 * return: 0 on success, -1 on fail
 */
int
ovs_create_volume(ovs_ctx_t *ctx,
                  const char *dev_name,
                  uint64_t size);

/*
 * Remove an Open vStorage volume
 * param ctx: Open vStorage context
 * param dev_name: Volume name
 * return: 0 on success, -1 on fail
 */
int
ovs_remove_volume(ovs_ctx_t *ctx,
                  const char *dev_name);


/*
 * Allocate buffer from the shared memory segment
 * param ctx: Open vStorage context
 * param size: Buffer size in bytes
 * return: Buffer pointer on success, or NULL on fail
 */
ovs_buffer_t*
ovs_allocate(ovs_ctx_t *ctx,
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
ovs_deallocate(ovs_ctx_t *ctx,
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
ovs_read(ovs_ctx_t *ctx,
         uint64_t gobjid_,
         void *buf,
         size_t nbytes,
         off_t offset);

/*
 * Write to a volume
 * param ctx: Open vStorage context
 * param buf: Shared memory buffer
 * param nbytes: Size to write in bytes
 * param offset: Offset to write in volume
 * return: Number of bytes actually written, -Error on fail
 */
ssize_t
ovs_write(ovs_ctx_t *ctx,
          uint64_t gobjid_,
          const void *buf,
          size_t nbytes,
          off_t offset);

/*
 * Syncronize a volume's in-core state with that on disk
 * param ctx: Open vStorage context
 * return: 0 on success, -1 on fail
 */
int
ovs_flush(ovs_ctx_t *ctx, uint64_t gobjid_);

/*
 * Get volume status
 * param ctx: Open vStorage context
 * param buf: Pointer to a stat structure
 * return: 0 on success, -1 on fail
 */
int
ovs_stat(ovs_ctx_t *ctx,
         struct stat *buf);

/*
 * Suspend until asynchronous I/O operation or timeout complete
 * param ctx: Open vStorage context
 * param ovs_aiocb: Pointer to an AIO Control Block structure
 * param timeout: Pointer to a timespec structure
 * return: 0 on success, -1 on fail
 */
int
ovs_aio_suspend(ovs_ctx_t *ctx,
                struct ovs_aiocb *ovs_aiocb,
                const struct timespec *timeout);

/*
 * Retrieve error status of asynchronous I/O operation
 * param ctx: Open vStorage context
 * param ovs_aiocb: Pointer to an AIO Control Block structure
 * return: 0 on success, -1 on fail
 */
int
ovs_aio_error(ovs_ctx_t *ctx,
              struct ovs_aiocb *ovs_aiocbp);

/*
 * Retrieve return status of asynchronous I/O operation
 * param ctx: Open vStorage context
 * param ovs_aiocb: Pointer to an AIO Control Block structure
 * return: Number of bytes returned based on the operation, -1 on fail
 */
ssize_t
ovs_aio_return(ovs_ctx_t *ctx,
               struct ovs_aiocb *ovs_aiocbp);

/*
 * Cancel an oustanding asynchronous I/O operation
 * param ctx: Open vStorage context
 * param ovs_aiocb: Pointer to an AIO Control Block structure
 * return: 0 on success, -1 on fail
 */
int
ovs_aio_cancel(ovs_ctx_t *ctx,
               struct ovs_aiocb *ovs_aiocbp);

/*
 * Finish an asynchronous I/O operation
 * param ctx: Open vStorage context
 * param ovs_aiocb: Pointer to an AIO Control Block structure
 * return: 0 on success, -1 on fail
 */
int
ovs_aio_finish(ovs_ctx_t *ctx,
               struct ovs_aiocb* ovs_aiocbp);

/*
 * Asynchronous read from a volume
 * param ctx: Open vStorage context
 * param ovs_aiocb: Pointer to an AIO Control Block structure
 * return: 0 on success, -1 on fail
 */
int
ovs_aio_read(ovs_ctx_t *ctx,
             const std::string  &gobjid,
             struct ovs_aiocb *ovs_aiocbp);

/*
 * Asynchronous write to a volume
 * param ctx: Open vStorage context
 * param ovs_aiocb: Pointer to an AIO Control Block structure
 * return: 0 on success, -1 on fail
 */
int
ovs_aio_write(ovs_ctx_t *ctx,
              const std::string &gobjid,
              struct ovs_aiocb *ovs_aiocbp);

/*
 * Asynchronous read from a volume with completion
 * param ctx: Open vStorage context
 * param ovs_aiocb: Pointer to an AIO Control Block structure
 * param completion: Pointer to a completion structure
 * return: 0 on success, -1 on fail
 */
int
ovs_aio_readcb(ovs_ctx_t *ctx,
               uint64_t gobjid_,
               struct ovs_aiocb *ovs_aiocbp,
               ovs_completion_t *completion);

/*
 * Asynchronous write to a volume with completion
 * param ctx: Open vStorage context
 * param ovs_aiocb: Pointer to an AIO Control Block structure
 * param completion: Pointer to a completion structure
 * return: 0 on success, -1 on fail
 */
int
ovs_aio_writecb(ovs_ctx_t *ctx,
                uint64_t gobjid_,
                struct ovs_aiocb *ovs_aiocbp,
                ovs_completion_t *completion);

/*
 * Asynchronously syncronize a volume's in-core state with that on disk with
 * completion
 * param ctx: Open vStorage context
 * param completion: Pointer to a completion structure
 * return: 0 on success, -1 on fail
 */
int
ovs_aio_flushcb(ovs_ctx_t *ctx,
                uint64_t gobjid_,
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

#ifdef __cplusplus
} //extern "C" endif
#endif

#endif // __LIB_OVS_VOLUMEDRIVER_H_
