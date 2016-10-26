#include <rora/RoraGateway.h>

int main(int argc, char* argv[])
{
  gobjfs::rora::RoraGateway gw;

  gw.init("./rora_gateway.conf");
  gw.run();
  gw.shutdown();
}
