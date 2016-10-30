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

#include "EPoller.h"

#include <cassert>  
#include <gobjfs_log.h>
#include <strings.h>


namespace gobjfs {
namespace os {

int EPoller::shutdownHandler(int fd, uintptr_t ctx) {
  EventFD* evfd = reinterpret_cast<EventFD*>(ctx);
  return evfd->readfd();
}

int EPoller::init() {

  int ret = -1;

  do {

    fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (fd_ < 0) {
      LOG(ERROR) << "epollfd create failed" << errno;
      close(fd_);
      fd_ = -1;
      ret = -errno;
      break;
    }

    try {
      shutdownPtr_ = std::unique_ptr<EventFD>(new EventFD());
    } catch (const std::exception& e) {
      LOG(ERROR) << "failed to create shutdown notifier " << e.what();
      close(fd_);
      fd_ = -1;
      break;
    }

    const uint64_t shutdownKey = reinterpret_cast<uint64_t>(shutdownPtr_.get());

    ret = addEvent(shutdownKey, shutdownPtr_->getfd(), EPOLLIN, 
        std::bind(&EPoller::shutdownHandler, this, 
          std::placeholders::_1,
          std::placeholders::_2));
    if (ret < 0) {
      close(fd_);
      fd_ = -1;
      shutdownPtr_.reset();
      break;
    }

    ret = 0;

  } while (0);

  if (ret == 0) {
    LOG(INFO) << "started epoller=" << (void*)this;
  }

  return ret;
}

int EPoller::shutdown() {

  if (shutdownPtr_) {

    {
      mustExit_ = true;
      // no thread can enter epoller::run now
      // but what if a thread exits on its own accord before shutdown
      // notification?
      // then the thread calling shutdown() could get stuck waiting 
      // to prevent that, this thread waits with a timeout
      int waitingTime = 1;
      while (numThreads_) {

        LOG(INFO) << "epoller=" << (void*)this << " shutting down.  Num threads remaining=" << numThreads_;
        shutdownPtr_->writefd();

        sleep(waitingTime);
        waitingTime = waitingTime << 1;
      }
    }
    // reset ptr AFTER thread has exited
    shutdownPtr_.reset();
    LOG(INFO) << "shutting down epoller=" << (void*)this;
  } else {
    LOG(WARNING) << "did not find shutdown event fd for epoller=" << (void*)this;
  }
  return 0;
}

EPoller::~EPoller() {

  shutdown();

  {
    std::unique_lock<std::mutex> l(eventListMutex_);
    eventList_.clear();
  }

  if (fd_ != -1) {
    close(fd_);
    fd_ = -1;
  }
}

int EPoller::getFD() const {
  return fd_;
}

/**
 * Create hierarchy of epoll fds
 * add fd of this EPoller to epoll_wait() of another EPoller
 */
int EPoller::addToAnotherEPoller(EPoller& other) {

  auto handler = std::bind(&EPoller::eventHandler, this, 
      std::placeholders::_1,
      std::placeholders::_2);

  return other.addEvent(reinterpret_cast<uint64_t>(this),
    getFD(),
    EPOLLIN | EPOLLOUT,
    std::move(handler));
}

/**
 * called from processEvent of another EPoller
 * @param fd
 * @param userData
 */
int EPoller::eventHandler(int fd, uint64_t userData) {
  assert(fd == fd_);
  assert(userData == (uint64_t)this);
  run(1);
  return 0;
}

/**
 * @param key
 */
int EPoller::processEvent(uint64_t key) {

  int handlerRet = 0;

  EPollCtx* ctx = (EPollCtx*)key;
  if (ctx->magic_ != MAGIC) {
    LOG(ERROR) << "received unknown event with ptr=" << ctx;
    handlerRet = -ENOENT;
  } else {
    handlerRet = ctx->handler_(ctx->fd_, ctx->userData_);
  }
  return handlerRet;
}

/**
 * can be called by any thread which wants to process 
 * events on this EPoller fd
 * TODO support timeout
 * @param numLoops to run, default is infinite
 */
int EPoller::run(int32_t numLoops) {

  {
    if (mustExit_) {
      LOG(WARNING) << "epoller is exiting..";
      return 0;
    } else {
      numThreads_ ++;
    }
  }
  
  uint64_t numConsecutiveNegativeWakeups = 0;
  while ((mustExit_ == false) && (numLoops != 0)) {

    epoll_event readyEvents[maxEventsPerPoll_];
    bzero(readyEvents, sizeof(epoll_event) * maxEventsPerPoll_);

    int numEvents = 0;

    do {
      numEvents = epoll_wait(fd_, readyEvents, maxEventsPerPoll_, -1);
      if (numEvents < 0) {
        numConsecutiveNegativeWakeups ++;
        if (numConsecutiveNegativeWakeups & 100) {
          LOG(WARNING) << "number of consecutive negative wakeups=" << numConsecutiveNegativeWakeups;
        }
      } else {
        numConsecutiveNegativeWakeups = 0;
      }
    } while ((numEvents < 0) && (errno == EINTR || errno == EAGAIN));

    if (numEvents < 0) {
      LOG(ERROR) << "epoll_wait=" << (void*)this << " got errno=" << errno;
      continue;
    } else if (numEvents > 0) {
      for (int i = 0; i < numEvents; i++) {
        epoll_event &thisEvent = readyEvents[i];
        int handlerRet = processEvent(thisEvent.data.u64);
        (void) handlerRet;
      }
    }

    if (numLoops > 0) {
      numLoops --;
    }
  }

  numThreads_ --;
  return 0;
}


/**
 * @param userData which will passed as arg to eventHandler
 * @param eventFD which is added to epoll_wait
 * @param readWrite can be EPOLLIN | EPOLLOUT
 * @param eventHandler function which is called on eventFD getting an event
 * @return 0 on success, negative errno on error
 */

int EPoller::addEvent(uint64_t userData, 
  int eventFD, 
  int readWrite, 
  EventHandler eventHandler) {

  // validate input
  if (eventFD < 0) {
    LOG(ERROR) << "eventFD=" << eventFD << " is invalid";
    return -EINVAL;
  }

  auto ctxptr = EPollCtxPtr(new EPollCtx(eventHandler, userData, eventFD));

  epoll_event eventInfo;
  bzero(&eventInfo, sizeof(eventInfo));

  eventInfo.events = readWrite | EPOLLET;
  eventInfo.data.u64 = reinterpret_cast<uint64_t>(ctxptr.get());

  int ret = epoll_ctl(fd_, EPOLL_CTL_ADD, eventFD, &eventInfo);
  if (ret < 0) {
    LOG(ERROR) << "Failed to add event.  errno=" << errno;
    ret = -errno;
  } else {
    std::unique_lock<std::mutex> l(eventListMutex_);
    eventList_.push_back(std::move(ctxptr));
  }

  return ret;
}

/**
 * @param userData which was used in addEvent
 * @param eventFD which was used in addEvent
 * @return 0 on success, negative errno on error
 */
int EPoller::dropEvent(uint64_t userData, int eventFD) {

  int delRet = epoll_ctl(fd_, EPOLL_CTL_DEL, eventFD, nullptr);

  if (delRet < 0) {
    LOG(ERROR) << "unable to delete existing event for fd=" << eventFD 
      << " userData=" << userData
      << " from epoll fd=" << fd_
      << " errno=" << errno;
    delRet = -errno;
  }

  bool found = false;
  {
    std::unique_lock<std::mutex> l(eventListMutex_);
    for (auto iter = eventList_.begin(); 
        iter != eventList_.end();
        iter ++) {

      EPollCtx* c = iter->get();
      if ((c->userData_ == userData) && (c->fd_ == eventFD)) {
        found = true;
        eventList_.erase(iter);
        break;
      }
    }
  }

  if (false == found) {
    LOG(ERROR) << "unable to delete event for fd=" << eventFD 
      << " userData=" << userData
      << " from eventList of size=" << eventList_.size();
    delRet = -ENOENT;
  }

  return delRet;
}

int EPoller::addTimer()
{
  return 0;
}

int EPoller::dropTimer()
{
  return 0;
}

}
}
