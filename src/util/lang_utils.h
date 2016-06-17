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

#include <memory> // unique_ptr

namespace gobjfs {

// double negation (!!) guarantees expr quickly turns into an "bool"
// http://stackoverflow.com/questions/248693/double-negation-in-c-code/249305#249305
#define gobjfs_unlikely(x) __builtin_expect(!!(x), 0)
#define gobjfs_likely(x) __builtin_expect(!!(x), 1)

#define GOBJFS_DISALLOW_COPY(CLASS)                                            \
  CLASS(const CLASS &) = delete;                                               \
  CLASS &operator=(const CLASS &) = delete;

#define GOBJFS_DISALLOW_MOVE(CLASS)                                            \
  CLASS(CLASS &&) = delete;                                                    \
  CLASS &operator=(CLASS &&) = delete;

#define Stringize(name) #name

template <typename T, typename... Args>
std::unique_ptr<T> make_unique(Args &&... args) {
  return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}
}
