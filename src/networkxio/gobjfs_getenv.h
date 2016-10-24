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

#include <type_traits>
#include <boost/lexical_cast.hpp>

namespace gobjfs {
namespace xio {

template <typename T>
static T getenv_with_default(const std::string &name,
                              const T &default_value) {
  typedef typename std::remove_cv<T>::type T_;

  static_assert(not(std::is_same<T_, uint8_t>::value or
                        std::is_same<T_, int8_t>::value or
                            std::is_same<T_, char>::value or
                                std::is_same<T_, unsigned char>::value),
                "this doesn't work with byte sized types");

  const char *val = getenv(name.c_str());
  if (val == nullptr) {
    return default_value;
  } else {
    try {
      return static_cast<T>(boost::lexical_cast<T>(val));
    }
    catch (...) {
      return default_value;
    }
  }
}

}
}
