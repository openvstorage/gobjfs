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

#include <string>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <gobjfs_log.h>

#include <iostream>
#include <fstream>

#include <assert.h>
#include <vector>
#include <string.h>
#include <strings.h>

#include <gIOExecFile.h>

#include <gobjfs_client.h>
#include <networkxio/NetworkXioServer.h>

#include <boost/program_options.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/record_ostream.hpp>

using namespace boost::program_options;
using namespace gobjfs::xio;
using namespace std;

namespace logging = boost::log;
namespace src = boost::log::sources;
namespace sinks = boost::log::sinks;
namespace keywords = boost::log::keywords;

struct Config {

  uint32_t startCore = 1;
  uint32_t numCores = 2;
  uint32_t queueDepth = 20;
  bool newInstance = true;

  std::string ipAddress;
  int port = 0;
  std::string transport;

  int readConfig(const std::string &configFileName) {
    options_description desc("allowed options");
    desc.add_options()
        ("queue_depth", value<uint32_t>(&queueDepth)->required(), "queue depth in ioexecutor")
        ("start_core", value<uint32_t>(&startCore)->required(), "starting core from which to reserve for ioexecutor")
        ("num_cores", value<uint32_t>(&numCores)->required(), "num cores to use for ioexecutor")
        ("new_instance", value<bool>(&newInstance), "new instance")

        ("ipaddress", value<std::string>(&ipAddress)->required(), "ip address")
        ("transport", value<std::string>(&transport)->required(), "transport is rdma or tcp")
        ("port", value<int>(&port)->required(), "port on which xio server running");

    std::ifstream configFile(configFileName);
    variables_map vm;
    store(parse_config_file(configFile, desc), vm);
    notify(vm);

    LOG(INFO)
        << "================================================================="
        << std::endl << "     BenchNetServer config" << std::endl
        << "================================================================="
        << std::endl;
    std::ostringstream s;
    for (const auto &it : vm) {
      s << it.first.c_str() << "=";
      auto &value = it.second.value();
      if (auto v = boost::any_cast<uint64_t>(&value))
        s << *v << std::endl;
      else if (auto v = boost::any_cast<uint32_t>(&value))
        s << *v << std::endl;
      else if (auto v = boost::any_cast<int>(&value))
        s << *v << std::endl;
      else if (auto v = boost::any_cast<std::string>(&value))
        s << *v << std::endl;
      else if (auto v = boost::any_cast<bool>(&value))
        s << *v << std::endl;
      else if (auto v = boost::any_cast<std::vector<std::string>>(&value)) {
        for (auto dirName : *v) {
          s << dirName << ",";
        }
        s << std::endl;
      } else
        s << "cannot interpret value " << std::endl;
    }
    LOG(INFO) << s.str();
    LOG(INFO)
        << "=================================================================="
        << std::endl;

    return 0;
  }
};

Config config;

static std::string configFileName = "bench_net_server.conf";

int main(int argc, char *argv[]) {

  // google::InitGoogleLogging(argv[0]); TODO logging
  namespace logging = boost::log;
  logging::core::get()->set_filter(logging::trivial::severity >=
      logging::trivial::info);

  std::string logFileName(argv[0]);
  logFileName += std::to_string(getpid()) + std::string("_%N.log");
	logging::add_file_log
	(
    keywords::file_name = logFileName,
    keywords::rotation_size = 10 * 1024 * 1024,
    keywords::time_based_rotation = sinks::file::rotation_at_time_point(0, 0, 0), 
    keywords::auto_flush = true,
    keywords::format = "[%TimeStamp%]: %Message%"
  );

  logging::add_common_attributes();// puts timestamp in log

  std::cout << "logs in " << logFileName << std::endl;

  if (argc > 1) {
      configFileName = argv[1];
  }

  {
    struct stat statbuf;
    int err = stat(configFileName.c_str(), &statbuf);
    if (err != 0) {
      LOG(ERROR) << "need a config file " << configFileName << " in current dir";
      exit(1);
    }
  }
  config.readConfig(configFileName);

  FileTranslatorFunc fileTranslatorFunc{nullptr};

  std::promise<void> pr;

  NetworkXioServer *xs =
      new NetworkXioServer(config.transport, config.ipAddress, config.port, 
          config.startCore, config.numCores, config.queueDepth, fileTranslatorFunc, config.newInstance);

  xs->run(pr);
}
