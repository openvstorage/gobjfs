#include <stdio.h> // malloc
#include <unistd.h> // write
#include <sys/socket.h>
#include <netinet/in.h> 
#include <arpa/inet.h> //  inet_addr
#include <string>
#include <fcntl.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sendfile.h>
#include <iostream>
#include <future>
#include <thread>
#include <random>
#include <glog/logging.h>

static int ConnectSocket(std::string& addr, int port, int& sock_fd)
{
  sock_fd = socket(PF_INET, SOCK_STREAM, 0); 

  struct sockaddr_in server_addr;
  socklen_t server_addr_size;

  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = inet_addr(addr.c_str());
  memset(server_addr.sin_zero, '\0', sizeof(server_addr.sin_zero));
  server_addr_size = sizeof(server_addr);

  int err = connect(sock_fd, (struct sockaddr*)&server_addr, server_addr_size);
  if (err < 0)  
  {
    close(sock_fd);
    return errno;
  }
  return err;
}

int socketFd = -1;
int devfd = -1;

const size_t diskSize = 700 * 1024 * 1024 * 1024;

void readThread(int index)
{
  std::mt19937 seedGen(getpid() + index);
  std::uniform_int_distribution<off_t> offgen(
      0, (700*1024 * 1024 * 1024)/4);

  off_t offset = 0;
  for (int i = 0; i < 1000000; i++)
  {
    offset = offgen(seedGen);

    ssize_t ret = sendfile(socketFd, devfd, &offset, 4096);
    if (ret != 4096) {
      std::cout << "failed err=" << errno;
      exit(1);
    }
    LOG_EVERY_N(INFO, 100) << "at off=" << offset;
  }
}

int main(int argc, char* argv[])
{
  if (argc == 1) {
    std::cout << "usage:" << argv[0] << " <port>" << std::endl;
    exit(1);
  }
  int port = atoi(argv[1]);
  const char* deviceName = "/dev/nvme0n1";
  std::string serverName = "127.0.0.1";

  int err = ConnectSocket(serverName, port, socketFd);

  devfd = open(deviceName, O_RDWR);

  std::vector<std::future<void>> futVec;

  for (int idx = 0; idx < 1; idx++)
  {
    auto fut = std::async(std::launch::async,
      readThread, idx);
    futVec.emplace_back(std::move(fut));
  }

  for (auto& elem : futVec)
  {
    elem.wait();
  }
}

