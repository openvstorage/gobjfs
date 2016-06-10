//#include "NetworkXioInterface.h"
#include "NetworkXioServer.h"
#include <string>
#include <assert.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <iostream>
#include <assert.h>
#include <vector>
#include <string.h>
#include <math.h>
#include <chrono>
#include <sys/time.h>

#include <mutex>
#include <atomic>
#include <condition_variable>
#include <thread>
#include <boost/program_options.hpp>
#include <fstream>
#include <gIOExecFile.h>
#include <strings.h>

using namespace boost::program_options;
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
    
    if (IOExecFileServiceInit(configFileName.c_str()) < 0) {
        LOG(ERROR) << "IOExecFileService init Failed";
        return -1;
    }
    NetworkXioServer *xs = new NetworkXioServer(Url);
    xs->run();
}
