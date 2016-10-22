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

namespace gobjfs {
namespace xio {

struct gbuffer;

class client_ctx;
typedef std::shared_ptr<client_ctx> client_ctx_ptr;

class client_ctx_attr;
typedef std::shared_ptr<client_ctx_attr> client_ctx_attr_ptr;

struct aio_request;

struct giocb {
  std::string filename;
  void *aio_buf;
  off_t aio_offset;
  size_t aio_nbytes;
  aio_request *request_;
};

/*
 * Create context attributes object
 * @param attr: Context attributes object
 * @return: This function always succeeds returning 0
 * @return: Context attributes object on success, or NULL on fail
 */
client_ctx_attr_ptr ctx_attr_new();

/*
 * Set transport type
 * @param attr: Context attributes object
 * @param transport: Transport string ("tcp" or "rdma)
 * @param host: Host string FQDN or IP address
 * @param port: TCP/RDMA port number
 * @return: 0 on success, -1 on fail
 */
int ctx_attr_set_transport(client_ctx_attr_ptr attr,
                           std::string const &transport,
                           std::string const &host, int port);

/*
 * Create gobjfs xio context
 * @param attr: Context attributes object
 * @return: gobjfs xio context on success, or NULL on fail
 */
client_ctx_ptr ctx_new(const client_ctx_attr_ptr attr);

/*
 * Initialize gobjfs xio context to talk to server
 * @param ctx: gobjfs xio context
 * @return: zero on success, or error code on fail
 */
int ctx_init(client_ctx_ptr ctx);

/**
 * Get stats for this connection
 * @param ctx : gobjfs xio context
 * @return : statistics on read requests
 */
std::string ctx_get_stats(client_ctx_ptr ctx);

/*
 * Check connection status
 * @param ctx: gobjfs xio context
 * @return: True if the client has been disconnected, false otherwise
 */
bool ctx_is_disconnected(client_ctx_ptr ctx);

/*
 * Allocate buffer from the shared memory segment
 * @param ctx: gobjfs xio context
 * @param size: Buffer size in bytes
 * @return: Buffer pointer on success, or NULL on fail
 */
gbuffer *gbuffer_allocate(client_ctx_ptr ctx, size_t size);

/* Retrieve pointer to buffer content
 * @param ptr: Pointer to buffer structure
 * @return: Buffer pointer on success, or NULL on fail
 */
void *gbuffer_data(gbuffer *ptr);

/* Retrieve size of buffer
 * @param ptr: Pointer to buffer structure
 * @return: Size of buffer on success, -1 on fail
 */
size_t gbuffer_size(gbuffer *ptr);

/*
 * Deallocate previously allocated buffer
 * @param ctx: gobjfs xio context
 * @param shptr: Buffer pointer
 * @return: 0 on success, -1 on fail
 */
int gbuffer_deallocate(client_ctx_ptr ctx, gbuffer *ptr);

/*
 * Read from a volume
 * @param ctx: gobjfs xio context
 * @param filename: file to read from
 * @param buf: Shared memory buffer
 * @param nbytes: Size to read in bytes
 * @param offset: Offset to read in volume
 * @return: Number of bytes actually read, -1 on fail
 */
ssize_t read(client_ctx_ptr ctx, const std::string &filename, void *buf,
             size_t nbytes, off_t offset);

/*
 * Suspend until asynchronous I/O operation or timeout complete
 * @param ctx: gobjfs xio context
 * @param giocb: Pointer to an AIO Control Block structure
 * @param timeout: Pointer to a timespec structure
 * @return: 0 on success, -1 on fail
 */
int aio_suspend(client_ctx_ptr ctx, giocb *giocb, const timespec *timeout);

/*
 * Suspend until asynchronous I/O operation or timeout complete
 * @param ctx: gobjfs xio context
 * @param giocb_vec: Pointer to vector of AIO Control Block structure
 * @param timeout: Pointer to a timespec structure
 * @return: 0 on success, -1 on fail
 */

int aio_suspendv(client_ctx_ptr ctx, const std::vector<giocb *> &giocbp_vec,
                 const timespec *timeout);
/*
 * Retrieve error status of asynchronous I/O operation
 * @param giocb: Pointer to an AIO Control Block structure
 * @return: 0 on success, -1 on fail
 */
int aio_error(giocb *giocbp);

/*
 * Retrieve return status of asynchronous I/O operation
 * @param giocb: Pointer to an AIO Control Block structure
 * @return: Number of bytes returned based on the operation, -1 on fail
 */
ssize_t aio_return(giocb *giocbp);

/*
 * Destroy internal resources associated to an asynchronous I/O operation
 * @param giocb: Pointer to an AIO Control Block structure
 * @return: 0 on success, -1 on fail
 */
int aio_finish(giocb *giocbp);

/*
 * Asynchronous read from a volume
 * @param ctx: gobjfs xio context
 * @param giocb: Pointer to an AIO Control Block structure
 * @return: 0 on success, -1 on fail
 */
int aio_read(client_ctx_ptr ctx, giocb *giocbp);

/*
 * Asynchronous readv from a volume
 * @param ctx: gobjfs xio context
 * @param giocb_vec: Pointer to vector of AIO Control Block structure
 * @return: 0 on success, -1 on fail
 */
int aio_readv(client_ctx_ptr ctx, const std::vector<giocb *> &giocbp_vec);

}
}
