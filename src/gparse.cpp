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

#include <fstream>
#include <glog/logging.h>
#include <gparse.h>

namespace po = boost::program_options;

int ParseConfigFile(const char *configFileName, IOExecutor::Config &config) {
  po::options_description desc("allowed options");
  desc.add_options()("ioexec.ctx_queue_depth",
                     po::value<uint32_t>(&config.queueDepth_)->required(),
                     "io depth of each context in IOExecutor")(
      "ioexec.cpu_core", po::value<std::vector<CoreId>>(&config.cpuCores_)
                             ->multitoken()
                             ->required(),
      "cpu cores dedicated to IO");
  std::ifstream configFile(configFileName);
  po::variables_map vm;

  int ret = 0;
  if (configFile.is_open()) {
    po::store(po::parse_config_file(configFile, desc), vm);
    po::notify(vm);
    VLOG(2) << "read config file=" << configFileName << "[ioexec]"
            << "ioexec.ctx_queue_depth" << config.queueDepth_;
    //<< "ioexec.cpu_core" << config.cpuCores_;
  } else {
    LOG(ERROR) << "Failed to open the file" << configFileName;
    ret = -1;
  }
  return ret;
}
