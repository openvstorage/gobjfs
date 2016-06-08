#include <stdio.h>
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <string.h>
#include <thread> 
#include <map> // inmemory map
#include <string> // key value

constexpr size_t MaxReadSize = 4096;
size_t MyPort = 65000;

int main(int argc, char* argv[])
{
  if (argc > 1) {
    MyPort = atoi(argv[1]);
  }
  int listenSocket;
  int newSocket;
  struct sockaddr_in serverAddr;
  struct sockaddr_storage serverStorage;

  socklen_t addrSize;

  listenSocket = socket(PF_INET, SOCK_STREAM, 0);

  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(MyPort);
  serverAddr.sin_addr.s_addr = inet_addr("127.0.0.1");
  memset(serverAddr.sin_zero, '\0', sizeof serverAddr.sin_zero);

  bind(listenSocket, (struct sockaddr *) &serverAddr, sizeof(serverAddr));

  if(listen(listenSocket,5)==0)
    printf("Listening on %d\n", MyPort);
  else {
    printf("Error\n");
    exit(1);
  }

  addrSize = sizeof(serverStorage);
  newSocket = accept(listenSocket,
    (struct sockaddr *) &serverStorage, &addrSize);

  std::cout << "got conn" << std::endl;
  char buf[MaxReadSize];

  bool eof = false;

  uint64_t numBytesRead = 0;
  uint64_t numReads = 0;

  while (!eof) {

    ssize_t readSize = read(newSocket, buf, sizeof(buf));
    if (readSize <= 0) {
      eof = true;
    } else {
      numBytesRead += readSize;
      numReads ++;
    }
  }

  close(listenSocket);
  close(newSocket);

  std::cout 
    << "numBytesRead=" << numBytesRead 
    << " numReads=" << numReads 
    << std::endl;

}
