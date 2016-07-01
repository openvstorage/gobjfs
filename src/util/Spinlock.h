#pragma once

#include <atomic>

namespace gobjfs {
namespace os {

/**
 * this class is "BasicLockable" as per boost
 * therefore, can be locked using lock_guard
 * {
 *   boost::lock_guard<decltype(lock)> l_(lock);
 * }
 *
 */
class Spinlock {
private:
  std::atomic_flag flag = ATOMIC_FLAG_INIT;

public:
  void lock() {
    while (flag.test_and_set(std::memory_order_acquire))
      ;
  }
  void unlock() { flag.clear(std::memory_order_release); }
};
}
}
