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

#include <cstdint> // uint
#include <thread>  // std::thread

// currently unused.  type to represent arbit member func ptr
template <class Any, typename... Args> struct vararg_method_ptr {
  typedef void (Any::*Function)(Args &&... args);
};

namespace gobjfs {

class Executor {
public:
  enum State : int8_t {
    NOT_STARTED,
    RUNNING,
    NO_MORE_INTAKE,
    FINAL_SHUTDOWN,
    TERMINATED
  };

  explicit Executor(const std::string &name, int16_t core = -1)
      : name_(name), core_(core) {}

  virtual ~Executor() {}

  virtual void execute() = 0;

  virtual void stop() = 0;

  int32_t getCore() const { return core_; }

protected:
  const std::string name_; // for debugging
  const int32_t core_;

  bool terminated_{false};
  State state_{State::NOT_STARTED};
};

} // namespace
