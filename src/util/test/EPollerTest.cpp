#include <gobjfs_log.h>
#include <gtest/gtest.h>
#include <future>
#include <thread>

#include <util/EventFD.h>
#include <util/TimerNotifier.h>
#include <util/EPoller.h>
#include <util/Pipe.h>
#include <util/lang_utils.h>


using gobjfs::os::EPoller;
using gobjfs::os::TimerNotifier;

/**
 * Check if EPoller start stop works
 */
TEST(EPoller, UpDown) {

  // verify basic op
  EPoller e1;

  int ret = e1.init();
  EXPECT_EQ(ret, 0);

  ret = e1.shutdown();
  EXPECT_EQ(ret, 0);
}


/**
 * Check if EPoller shutdown from another thread works
 */
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

static int readfunc(int fd, uintptr_t userData) {
  int* numTimesPtr = reinterpret_cast<int*>(userData);
  *numTimesPtr += EventFD::readfd(fd);
  return 0;
}

/**
 * Check if EventFD interops with EPoller
 */
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

static int timerfunc(int fd, uintptr_t userData) {
  uint64_t* numTimesPtr = reinterpret_cast<uint64_t*>(userData);
  uint64_t count = 0;
  int ret = TimerNotifier::recv(fd, count);
  if (ret == 0) {
    *numTimesPtr += count;
  }
  return 0;
}

/**
 * Check if TimerNotifier interops with EPoller
 */
TEST(EPoller, WithTimerNotifier) {

  EPoller e1;

  int ret = e1.init();
  EXPECT_EQ(ret, 0);

  int timerIntervalSec = 1;
  TimerNotifier t(timerIntervalSec, 0);

  uint64_t numTimesCalled = 0;

  ret = e1.addEvent(reinterpret_cast<uintptr_t>(&numTimesCalled), t.getFD(), EPOLLIN, timerfunc);
  EXPECT_EQ(ret, 0);

  sleep(timerIntervalSec); // sleep more than the timer

  // run loop once
  ret = e1.run(1);
  EXPECT_EQ(ret, 0);

  // check if timer handler was called
  EXPECT_GE(numTimesCalled, 1);

  // drop event which was added
  ret = e1.dropEvent(reinterpret_cast<uintptr_t>(&numTimesCalled), t.getFD());
  EXPECT_EQ(ret, 0);

  ret = e1.shutdown();
  EXPECT_EQ(ret, 0);
}
