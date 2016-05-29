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

#include <malloc.h>

extern "C" {

// @params size to which all blocks should be aligned
int gMempool_init(size_t alignSize);

void *gMempool_alloc(size_t sizeRequired);

void gMempool_free(void *memptr);

// @returns  number of allocs and frees + bytes allocated
//           cannot track bytesFreed since free() is not taking size
void gMempool_getStats(char *buffer, size_t len);
}

// expose stats
