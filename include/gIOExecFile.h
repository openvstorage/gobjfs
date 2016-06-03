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

#include <gcommon.h>
#include <stdint.h> // uint64
#include <stdlib.h>
#include <sys/uio.h>

struct IOExecServiceInt;
typedef IOExecServiceInt *IOExecServiceHandle;

struct IOExecFileInt;
typedef IOExecFileInt *IOExecFileHandle;

struct gIOExecFragment {
  gCompletionID completionId; // Caller specifies completionID
  off_t offset;               // Offset of the file fragment within the file.
  size_t size;                // Fragment Length
                              /*
                              * For Read API:  If passed NULL, will be allocated by Callee (for each read
                              * fragment), and caller must free it.
                              * For Write API : Caller passes the write buffer here.
                              * In both cases, memory is freed on calling gIOBatchFree
                              */
  caddr_t addr;
};

struct gIOBatch {
  size_t count;

  // variable sized array of "count" items
  gIOExecFragment array[0];
};

// allocate a gIOBatch
// It will set gIOExecFragment to all zeroes
gIOBatch *gIOBatchAlloc(size_t count);

// free a gIOBatch, including the fragments in it
void gIOBatchFree(gIOBatch *ptr);

// ========================================

struct gIOStatusBatch {
  size_t count;

  // variable sized array of "count" items
  gIOStatus array[0];
};

// Allocate an array of gIOStatusBatch
gIOStatusBatch *gIOStatusBatchAlloc(int count);

// free array
void gIOStatusBatchFree(gIOStatusBatch *ptr);

// ========================================

IOExecServiceHandle IOExecFileServiceInit(const char *configFileName);

int32_t IOExecFileServiceDestroy(IOExecServiceHandle);

IOExecFileHandle IOExecFileOpen(IOExecServiceHandle serviceHandle,
                                const char *filename, int32_t flags);

int32_t IOExecFileClose(IOExecFileHandle FileHandle);

int32_t IOExecFileTruncate(IOExecFileHandle FileHandle, size_t newSize);

struct IOExecEventFdInt;
typedef IOExecEventFdInt *IOExecEventFdHandle;

IOExecEventFdHandle IOExecEventFdOpen(IOExecServiceHandle serviceHandle);

int32_t IOExecEventFdClose(IOExecEventFdHandle eventFdHandle);

int IOExecEventFdGetReadFd(IOExecEventFdHandle eventFdPtr);

// hidden API to retrieve number of configured IOExecutors
int32_t IOExecGetNumExecutors(IOExecServiceHandle serviceHandle);

// return the number of bytes filled in buffer
int32_t IOExecGetStats(IOExecServiceHandle serviceHandle, char* buf,
  int32_t len);

/**
 * @param fileHandle file returned by IOExecFileOpen
 * @param pIOBatch batch containing offset, size and buffer to write
 * @param fd the pipe on which callback notification should be sent
 *           when job is completed
 */
int32_t IOExecFileWrite(IOExecFileHandle fileHandle, const gIOBatch *pIOBatch,
                        IOExecEventFdHandle eventFdHandle);

/**
 * @param fileHandle file returned by IOExecFileOpen
 * @param pIOBatch batch containing offset, size and buffer to read
 * @param fd the pipe on which callback notification should be sent
 *           when job is completed
 */
int32_t IOExecFileRead(IOExecFileHandle fileHandle, const gIOBatch *pIOBatch,
                       IOExecEventFdHandle eventFdHandle);

/**
 *
 */
int32_t IOExecFileDelete(IOExecServiceHandle serviceHandle,
                         const char *filename, gCompletionID completionId,
                         IOExecEventFdHandle eventFdHandle);

int32_t IOExecFileDeleteSync(IOExecServiceHandle serviceHandle,
                             const char *filename);

// C API
#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

typedef IOExecServiceInt *service_handle_t;

typedef IOExecFileInt *handle_t;

typedef IOExecEventFdInt *event_t;

typedef gIOBatch batch_t;

typedef gCompletionID completion_id_t;

typedef void *status_t;

EXTERNC {
  
  // @param full path of config file 
  // @return handle to service, else NULL pointer on error
  service_handle_t gobjfs_ioexecfile_service_init(const char *);

  // @param handle returned from "service_init"
  // @return 0 on success, else negative number
  int32_t gobjfs_ioexecfile_service_destroy(service_handle_t);

  // @param handle returned from "service_init"
  // @param absolute path of file to open
  // @param file flags as in unix (O_RDWR, O_CREAT, etc)
  // @return handle to opened file, else NULL pointer on error
  handle_t gobjfs_ioexecfile_file_open(service_handle_t, const char *, int);

  // @param handle returned by "file_open"
  // @param batch allocated by "batch_alloc"
  // @param pipe handle returned from "event_fd_open" 
  // @return 0 on successful submit, else negative number
  int32_t gobjfs_ioexecfile_file_write(handle_t, const batch_t *, event_t evfd);
  // @param handle returned by "file_open"
  // @param batch allocated by "batch_alloc"
  // @param pipe handle returned from "event_fd_open" 
  // @return 0 on successful submit, else negative number
  int32_t gobjfs_ioexecfile_file_read(handle_t, batch_t *, event_t evfd);

  // @param handle returned from "file_open"
  // @param name of file to delete
  // @param completion id to be returned in callback
  // @param pipe on which completion id will be returned
  // @return 0 on successful submit, else negative number
  int32_t gobjfs_ioexecfile_file_delete(service_handle_t, const char *,
                                        completion_id_t, event_t);

  // @param handle returned from "file_open"
  // @return 0 on successful close, else negative number
  int32_t gobjfs_ioexecfile_file_close(handle_t);

  // @param handle returned from "service_init"
  // @return handle to pipe, else NULL ptr on error
  event_t gobjfs_ioexecfile_event_fd_open(service_handle_t);

  // @param handle returned from "event_fd_open"
  // @return 0 on successful close, else negative number
  int32_t gobjfs_ioexecfile_event_fd_close(event_t);

  // @param handle returned from "event_fd_open"
  // @return for valid handle, fd which is greater than or equal to 0
  //      for invalid handle, returns -1
  int gobjfs_ioexecfile_event_fd_get_read_fd(event_t);

  // @param number of fragments in batch 
  // @return allocated batch, else NULL ptr on error
  batch_t *gobjfs_batch_alloc(int);

  // debugging helpers
  void gobjfs_debug_fragment(const void *);
  void gobjfs_debug_batch(const batch_t *);

  // @param buffer to fill 
  // @param the length of the buffer
  // @return number of bytes filled in buffer
  //   negative number on error
  int32_t gobjfs_ioexecfile_service_getstats(service_handle_t, char *buffer, 
    int32_t len);
}
