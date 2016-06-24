#include <gobjfs_log.h>
#include <gtest/gtest.h>

#include <util/SemaphoreWrapper.h>

#include <sys/epoll.h>

using gobjfs::os::SemaphoreWrapper;

TEST(SemaphoreWrapper, InitZero) {
  int epollFD = epoll_create1(0);

  SemaphoreWrapper s;
  auto ret = s.init(0, epollFD);
  EXPECT_EQ(ret, 0);

  for (int i = 0; i < 10; i++) {
    s.wakeup();
  }

  for (int i = 0; i < 10; i++) {
    s.pause();
  }

  // successful completion is itself sign of test working
}

TEST(SemaphoreWrapper, InitNonZero) {
  int epollFD = epoll_create1(0);

  SemaphoreWrapper s;
  auto ret = s.init(10, epollFD);
  EXPECT_EQ(ret, 0);

  for (int i = 0; i < 10; i++) {
    s.pause();
  }

  // successful completion is itself sign of test working
}
