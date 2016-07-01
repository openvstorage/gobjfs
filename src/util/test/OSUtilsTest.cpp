#include <gobjfs_log.h>
#include <gtest/gtest.h>

#include <util/os_utils.h>

#include <sys/epoll.h>
#include <sys/types.h>
#include <unistd.h>

using gobjfs::os::IsFdOpen;
using gobjfs::os::RoundToNext512;
using gobjfs::os::IsDirectIOAligned;

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

TEST(OSUtils, IsDirectIOAligned) {

  {
    bool ret = IsDirectIOAligned(0);
    EXPECT_TRUE(ret);
  }
  {
    bool ret = IsDirectIOAligned(511);
    EXPECT_FALSE(ret);
  }
  {
    bool ret = IsDirectIOAligned(512);
    EXPECT_TRUE(ret);
  }
  {
    bool ret = IsDirectIOAligned(4096);
    EXPECT_TRUE(ret);
  }
  {
    bool ret = IsDirectIOAligned(65535);
    EXPECT_FALSE(ret);
  }
}

TEST(OSUtils, RoundToNext512) {

  {
    uint64_t newVal = RoundToNext512(0);
    EXPECT_EQ(newVal, 0);
  }

  {
    uint64_t newVal = RoundToNext512(511);
    EXPECT_EQ(newVal, 512);
  }

  {
    uint64_t newVal = RoundToNext512(512);
    EXPECT_EQ(newVal, 512);
  }

  {
    uint64_t newVal = RoundToNext512(513);
    EXPECT_EQ(newVal, 1024);
  }
}
