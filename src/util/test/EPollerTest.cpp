#include <gobjfs_log.h>
#include <gtest/gtest.h>
#include <future>
#include <thread>

#include <util/EventFD.h>
#include <util/TimerNotifier.h>
#include <util/EPoller.h>
#include <util/Pipe.h>
#include <util/os_utils.h>
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
  uint64_t* numTimesPtr = reinterpret_cast<uint64_t*>(userData);
  int ret = EventFD::readfd(fd);
  if (ret  >= 0) {
    (*numTimesPtr) += ret;
  }
  std::ostringstream os;
  os << gettid() << " sees " << (*numTimesPtr) << "," << ret << std::endl;
  std::cout << os.str();
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

struct DummyData {
  int opcode_;
  int arg_;
};

static int pipe_read_func(int fd, uintptr_t userData) {
  uint64_t* numTimesPtr = reinterpret_cast<uint64_t*>(userData);
  DummyData d;
  ssize_t ret = read(fd, &d, sizeof(d));
  EXPECT_EQ(ret, sizeof(d));
  EXPECT_EQ(d.opcode_, 3);
  EXPECT_EQ(d.arg_, 4);
  (*numTimesPtr) ++;
  return 0;
}

/**
 * Check if Pipe interops with EPoller
 */
TEST(EPoller, WithPipe) {

  EPoller e1;

  int ret = e1.init();
  EXPECT_EQ(ret, 0);

  PipeUPtr pipe = gobjfs::make_unique<Pipe>();

  uint64_t numTimesCalled = 0;

  ret = e1.addEvent(reinterpret_cast<uintptr_t>(&numTimesCalled), pipe->getReadFD(), EPOLLIN, pipe_read_func);
  EXPECT_EQ(ret, 0);

  int write_fd = pipe->getWriteFD();

  auto fut = std::async(std::launch::async, [&write_fd] () {
 
      sleep(5); // wait for epoll to hang in main thread

      DummyData d;
      d.opcode_ = 3;
      d.arg_ = 4;
      ssize_t writeSz = write(write_fd, &d, sizeof(d));
      EXPECT_EQ(writeSz, sizeof(d));
    });

  // run loop once
  ret = e1.run(1);
  EXPECT_EQ(ret, 0);

  // check if timer handler was called
  EXPECT_GE(numTimesCalled, 1);

  // drop event which was added
  ret = e1.dropEvent(reinterpret_cast<uintptr_t>(&numTimesCalled), pipe->getReadFD());
  EXPECT_EQ(ret, 0);

  ret = e1.shutdown();
  EXPECT_EQ(ret, 0);
}

void epollFunc(EPoller *e1, bool *stopping) {

  while (!(*stopping)) {
    // run loop once
    int ret = e1->run(1);
    EXPECT_EQ(ret, 0);
  }
}

struct MultiThrTestCtx {
  uint64_t numTimesCalled = 0;
  EventFD evfd;
};

static int multithr_readfunc(int fd, uintptr_t userData) {
  MultiThrTestCtx* ctx = reinterpret_cast<MultiThrTestCtx*>(userData);
  int ret = EventFD::readfd(fd, &ctx->evfd);
  if (ret  >= 0) {
    ctx->numTimesCalled += ret;
  }
  std::ostringstream os;
  os << gettid() << " sees " << ctx->numTimesCalled << "," << ret << std::endl;
  std::cout << os.str();
  return 0;
}

/**
 * Check if multiple threads can use same EventFD with EPoller
 */
TEST(EPoller, MultiThrWithEventFD) {

  EPoller e1;

  int ret = e1.init();
  EXPECT_EQ(ret, 0);

  MultiThrTestCtx ctx;

  ret = e1.addEvent(reinterpret_cast<uintptr_t>(&ctx), (int)ctx.evfd, EPOLLIN, multithr_readfunc);
  EXPECT_EQ(ret, 0);

  int numThr = 10;

  bool stopping = false;
  std::vector<std::future<void>> futVec;
  for (int i = 0; i < numThr; i++) {
    auto fut = std::async(std::launch::async, std::bind(epollFunc, &e1, &stopping));
    futVec.push_back(std::move(fut));
  }

  int numWrites = 100;
  int numTimes = 5;
  // write eventfd
  for (int i = 0; i < numTimes; i++) {
    for (int j = 0; j < numWrites; j++) {
      ctx.evfd.writefd();
    }
    sleep(1);
  }

  stopping = true;
  // first shutdown the epoller
  ret = e1.shutdown();
  EXPECT_EQ(ret, 0);

  // then check if threads have exited
  for (auto& fut : futVec) {
    fut.wait();
  }
  futVec.clear();

  std::cout << ctx.evfd.stats_.ToString() << std::endl;

  EXPECT_LE(ctx.evfd.stats_.read_eintr_, numThr);
  EXPECT_LE(ctx.evfd.stats_.read_eagain_, numTimes * numThr);
  // assert that only one read should occur corresponding to one write 
  EXPECT_LE(ctx.evfd.stats_.read_ctr_.numSamples_, numTimes * numWrites);

  // check if read handler was called
  EXPECT_LE(ctx.numTimesCalled, numTimes * numWrites);

  // drop event which was added
  ret = e1.dropEvent(reinterpret_cast<uintptr_t>(&ctx.numTimesCalled), (int)ctx.evfd);
  EXPECT_EQ(ret, 0);

}
