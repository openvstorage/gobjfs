/*
Copyright (C) 2016 iNuron NV

This file is part of Open vStorage Open Source Edition (OSE), as available from


    http://www.openvstorage.org and
    http://www.openvstorage.com.

This file is free software; you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
as published by the Free Software Foundation, in version 3 as it comes
in the <LICENSE.txt> file of the Open vStorage OSE distribution.

Open vStorage is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY of any kind.
*/
#pragma once

#include <sys/epoll.h> // epoll
#include <list> 
#include <functional> // std::function
#include <mutex> // unique_lock
#include <memory>  // unique_ptr
#include <atomic>  // atomic

#include <util/EventFD.h>  // EventFD
#include <util/ConditionWrapper.h>  // EventFD

namespace gobjfs {
namespace os {

/**
 * abstraction around epoll event loop
 */
class EPoller {

  static constexpr int MAX_EVENTS_PER_POLL = 20;

  int fd_ = -1; // on which epoll_wait done
  int maxEventsPerPoll_ = MAX_EVENTS_PER_POLL;

  // need an EventFD to wake thread from epoll
  std::unique_ptr<EventFD> shutdownPtr_;

  // set at start of shutdown to prevent more threads
  bool mustExit_ = false;

  std::atomic<uint32_t> numThreads_{ 0 };

  // handles will get fd and ctx as args
  typedef std::function<int(int, uintptr_t)> EventHandler;

  static constexpr int MAGIC = 0xDEAFBABE;

  struct EPollCtx {
    const EventHandler handler_;
    const uint64_t userData_;
    const int fd_;
    const int magic_ = MAGIC;

    EPollCtx(const EventHandler& handler, uint64_t userData, int fd)
      : handler_(handler), userData_(userData), fd_(fd) {}
  };

  typedef std::unique_ptr<EPollCtx> EPollCtxPtr;

  std::mutex eventListMutex_;
  std::list<EPollCtxPtr> eventList_;

  int shutdownHandler(int fd, uintptr_t ctx);

  // TODO
  // timers
  // stats
  // getter 
  public:

  int init();

  int shutdown();

  ~EPoller();

  int getFD() const;

  /**
   * Create hierarchy of epoll fds
   * add fd of this EPoller to epoll_wait() of another EPoller
   */
  int addToAnotherEPoller(EPoller& other);

  /**
   * called from processEvent of upper-level EPoller
   * @param fd
   * @param userData
   */
  int eventHandler(int fd, uint64_t userData);

  /**
   * @param key
   */
  int processEvent(uint64_t key);

  /**
   * This method can be called by any thread which wants to process 
   *   events on this EPoller fd
   * @param numLoops to run, default is infinite
   */
  int run(int32_t numLoops = -1);
 
  /**
   * @param userData which will passed as arg to eventHandler
   * @param eventFD which is added to epoll_wait
   * @param readWrite can be EPOLLIN | EPOLLOUT
   * @param eventHandler function which is called on eventFD getting an event
   * @return 0 on success, negative errno on error
   */

  int addEvent(uintptr_t userData, 
    int eventFD, 
    int readWrite, 
    EventHandler eventHandler);

  /**
   * @param userData which was used in addEvent
   * @param eventFD which was used in addEvent
   * @return 0 on success, negative errno on error
   */
  int dropEvent(uint64_t userData, int eventFD);

  int addTimer();

  int dropTimer();

};

}
}
