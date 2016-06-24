#include <gobjfs_log.h>
#include <gtest/gtest.h>

#include <util/ShutdownNotifier.h>

#include <sys/epoll.h>

using gobjfs::os::ShutdownNotifier;

TEST(ShutdownNotifier, ReadWrite) {
  int epollFD = epoll_create1(0);

  ShutdownNotifier f;
  f.init(epollFD);

  for (int i = 0; i < 10; i++) {
    f.send();
  }

  uint64_t counter;
  f.recv(counter);

  EXPECT_EQ(counter, 1);

  auto ret = f.destroy();
  EXPECT_EQ(ret, 0);

  close(epollFD);
}
