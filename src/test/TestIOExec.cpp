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

#include <IOExecutor.h>

using gobjfs::async::FileOp;
using gobjfs::async::FilerJob;
using gobjfs::async::IOExecutor;
using gobjfs::async::IOExecutorSPtr;

IOExecutor::Config config;

int main(int argc, char *argv[]) {
  std::string configFileName = "./iobasic.conf";
  if (argc > 1) {
    configFileName = argv[1];
  }

  // Read config file
  // parse config file

  /*
   * Initialize ioexec cores equal to number of CPUs
   * Global Data structure containing ioexecVec = #CPU Cores
   * Done as part of hd = IOExecutor_init();
   */
  std::vector<IOExecutorSPtr> ioexecVec;
  for (uint32_t core = 0; core < NumCores; core++) {
    ioexecVec.emplace_back(
        (std::make_shared<IOEXecutor>, "dummy", core, config));
  }

  // Invoke Threads
  FileHandler *fh = new FileHandler("/dev/xyz", );
}
