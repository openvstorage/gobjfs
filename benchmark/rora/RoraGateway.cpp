#include <rora/RoraGateway.h>
#include <signal.h>
#include <glog/logging.h>

gobjfs::rora::RoraGateway gw;

void sigintHandler(int dummy) {
  gw.shutdown();
}

int main(int argc, char* argv[])
{
  google::InitGoogleLogging(argv[0]);

  signal(SIGINT, sigintHandler);
  gw.init("./rora_gateway.conf");
  gw.run();
}
