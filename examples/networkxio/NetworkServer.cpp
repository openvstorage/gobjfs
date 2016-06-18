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

#include <glog/logging.h>

#include <iostream>
#include <fstream>

#include <assert.h>
#include <vector>
#include <string.h>
#include <strings.h>

#include <gIOExecFile.h>

#include <gobjfs_client.h>
#include <networkxio/NetworkXioServer.h>


using namespace gobjfs::xio;
using namespace std;

int main(int argc, char *argv[]) {

    string Url = "tcp://127.0.0.1:21321";

    // log files are in /tmp
    google::InitGoogleLogging(argv[0]);

    std::string configFileName = "./gioexecfile.conf";

    if (argc > 1) {
      configFileName = argv[1];
    }
    std::promise<void> _pr;
    NetworkXioServer *xs = new NetworkXioServer(Url, configFileName);
    xs->run(_pr);
}
