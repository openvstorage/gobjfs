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

struct IOExecEventFdInt;
typedef IOExecEventFdInt *IOExecEventFdHandle;

IOExecEventFdHandle IOExecEventFdOpen(IOExecServiceHandle serviceHandle);

int32_t IOExecEventFdClose(IOExecEventFdHandle eventFdHandle);

int IOExecEventFdGetReadFd(IOExecEventFdHandle eventFdPtr);

// hidden API to retrieve number of configured IOExecutors
int32_t IOExecGetNumExecutors(IOExecServiceHandle serviceHandle);

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

  service_handle_t gobjfs_ioexecfile_service_init(const char *);

  int32_t gobjfs_ioexecfile_service_destroy(service_handle_t);

  handle_t gobjfs_ioexecfile_file_open(service_handle_t, const char *, int);

  int32_t gobjfs_ioexecfile_file_write(handle_t, const batch_t *, event_t evfd);
  int32_t gobjfs_ioexecfile_file_read(handle_t, batch_t *, event_t evfd);
  int32_t gobjfs_ioexecfile_file_delete(service_handle_t, const char *,
                                        completion_id_t, event_t);

  int32_t gobjfs_ioexecfile_file_close(handle_t);

  event_t gobjfs_ioexecfile_event_fd_open(service_handle_t);
  int32_t gobjfs_ioexecfile_event_fd_close(event_t);
  int gobjfs_ioexecfile_event_fd_get_read_fd(event_t);

  batch_t *gobjfs_batch_alloc(int);

  // debugging helpers
  void gobjfs_debug_fragment(const void *);
  void gobjfs_debug_batch(const batch_t *);
}
