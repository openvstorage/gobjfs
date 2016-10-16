#include <gobjfs_log.h>
#include <gtest/gtest.h>

#include <util/EventFD.h>
#include <util/lang_utils.h>

#include <sys/epoll.h>


TEST(EventFD, ReadWrite) {
  int epollFD = epoll_create1(0);

  auto evptr = gobjfs::make_unique<EventFD>();

  epoll_event epollEvent;
  bzero(&epollEvent, sizeof(epollEvent));
  epollEvent.data.ptr = this;
   
  epollEvent.events = EPOLLIN | EPOLLPRI;
  int ret = epoll_ctl(epollFD, EPOLL_CTL_ADD, (int)(*evptr), &epollEvent);
  EXPECT_EQ(ret, 0);

  for (int i = 0; i < 10; i++) {
    evptr->writefd();
  }

  int counter = evptr->readfd();

  EXPECT_EQ(counter, 10);

  evptr.reset();
  ret = close(epollFD);
  EXPECT_EQ(ret, 0);
}
