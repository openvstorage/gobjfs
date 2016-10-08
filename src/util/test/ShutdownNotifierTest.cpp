#include <gobjfs_log.h>
#include <gtest/gtest.h>

#include <util/ShutdownNotifier.h>

#include <sys/epoll.h>

using gobjfs::os::ShutdownNotifier;

TEST(ShutdownNotifier, ReadWrite) {
  int epollFD = epoll_create1(0);

  ShutdownNotifier f;
  f.init();

  epoll_event epollEvent;
  bzero(&epollEvent, sizeof(epollEvent));
  epollEvent.data.ptr = this;
   
  epollEvent.events = EPOLLIN | EPOLLPRI;
  int ret = epoll_ctl(epollFD, EPOLL_CTL_ADD, f.getFD(), &epollEvent);
  EXPECT_EQ(ret, 0);

  for (int i = 0; i < 10; i++) {
    f.send();
  }

  uint64_t counter;
  f.recv(counter);

  EXPECT_EQ(counter, 1);

  ret = f.destroy();
  EXPECT_EQ(ret, 0);

  close(epollFD);
}
