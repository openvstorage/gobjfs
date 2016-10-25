#include <gobjfs_log.h>
#include <gtest/gtest.h>
#include <future>
#include <thread>

#include <util/EventFD.h>
#include <util/EPoller.h>
#include <util/lang_utils.h>


using gobjfs::os::EPoller;

TEST(EPoller, UpDown) {

  // verify basic op
  EPoller e1;

  int ret = e1.init();
  EXPECT_EQ(ret, 0);

  ret = e1.shutdown();
  EXPECT_EQ(ret, 0);
}


static int readfunc(int fd, uint64_t userData) {
  int* numTimesPtr = reinterpret_cast<int*>(userData);
  *numTimesPtr += EventFD::readfd(fd);
  return 0;
}

TEST(EPoller, shutdown) {

  EPoller e1;

  int ret = e1.init();
  EXPECT_EQ(ret, 0);

  // shutdown the epoller after 10 sec
  auto fut = std::async(std::launch::async, 
      [&e1] () { 
        sleep(10); 
        e1.shutdown(); }
      );

  // run loop infinite
  ret = e1.run(-1);
  EXPECT_EQ(ret, 0);
  
  // test succeeds if it exits !
}

TEST(EPoller, WithEventFD) {

  EPoller e1;

  int ret = e1.init();
  EXPECT_EQ(ret, 0);

  EventFD evfd;

  uint64_t numTimesCalled = 0;

  ret = e1.addEvent(reinterpret_cast<uintptr_t>(&numTimesCalled), (int)evfd, EPOLLIN, readfunc);
  EXPECT_EQ(ret, 0);

  int numLoops = 10;

  // write to fd n times
  for (int i = 0; i < numLoops; i++) {
    evfd.writefd();
  }

  // run loop once
  ret = e1.run(1);
  EXPECT_EQ(ret, 0);

  // check if read handler was called
  EXPECT_EQ(numTimesCalled, numLoops);

  // drop event which was added
  ret = e1.dropEvent(reinterpret_cast<uintptr_t>(&numTimesCalled), (int)evfd);
  EXPECT_EQ(ret, 0);

  ret = e1.shutdown();
  EXPECT_EQ(ret, 0);
}
