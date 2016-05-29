#include <glog/logging.h>
#include <gtest/gtest.h>

#include <util/os_utils.h>

#include <sys/epoll.h>
#include <sys/types.h>
#include <unistd.h>

using gobjfs::os::IsFdOpen;

TEST(OSUtils, epollIsOpen) {
  int fd = epoll_create1(0);

  bool ret = IsFdOpen(fd);
  EXPECT_TRUE(ret);

  close(fd);
  ret = IsFdOpen(fd);
  EXPECT_FALSE(ret);
}

TEST(OSUtils, fileIsOpen) {
  std::string file = "/tmp/osutil_" + std::to_string(getpid());
  int fd = open(file.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

  bool ret = IsFdOpen(fd);
  EXPECT_TRUE(ret);

  close(fd);
  ret = IsFdOpen(fd);
  EXPECT_FALSE(ret);
}
