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

#include <stdint.h> // uint32_t

typedef int gOptions; /* Accepts O_RDWR, O_RDONLY or O_WRONLY */
typedef uint32_t gVersionID;
typedef uint32_t gContainerID;
typedef uint32_t gSegmentID;
typedef uint32_t gOffset;
typedef uint64_t gObjectID;
typedef uint64_t gCompletionID;

struct gIOStatus {
  gCompletionID completionId;
  int32_t errorCode;
  int32_t reserved; // for consistent padding
} __attribute__((packed, aligned(8)));

