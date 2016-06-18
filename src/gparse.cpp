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

#include <gparse.h>

#include <fstream>
#include <glog/logging.h>
#include <boost/program_options.hpp>

#include <boost/version.hpp>

namespace po = boost::program_options;

int ParseConfigFile(const char *configFileName, IOExecutor::Config &config) {

  // print boost compile time version for diagnostic purposes
  // in case it differs from link time version
  LOG(INFO) << "Compile-time Boost version is "     
    << BOOST_VERSION / 100000     << "."  // major version
    << BOOST_VERSION / 100 % 1000 << "."  // minor version
    << BOOST_VERSION % 100;                // patch level

  po::options_description desc("allowed options");

  // add options needed by IOExecutor
  config.addOptions(desc);
    
  // add FileDistributor mountpoint
  // TODO supply this to IOExecFileServiceInit
  std::string mountPoint;
  desc.add_options()
    ("mount_point", po::value<std::string>(&mountPoint), "mountpoint");

  std::ifstream configFile(configFileName);

  po::variables_map vm;

  int ret = 0;
  if (configFile.is_open()) {
    auto parsed_options = po::parse_config_file(configFile, desc);
    po::store(parsed_options, vm);
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
