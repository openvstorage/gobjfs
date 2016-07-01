#include <gobjfs_log.h>
#include <gtest/gtest.h>
#include <util/Stats.h>

#include <sstream>

using gobjfs::stats::StatsCounter;
using gobjfs::stats::Histogram;
using gobjfs::stats::MinValue;
using gobjfs::stats::MaxValue;

TEST(StatsCounter, check1) {
  StatsCounter<int64_t> a;

  for (int64_t i = 0; i < 10; i++) {
    a = i;
  }

  std::ostringstream s;
  s << a;
  LOG(INFO) << s.str();
  EXPECT_EQ(
      s.str(),
      "{\"min\":0,\"avg\":4.5,\"stddev\":3.02765,\"max\":9,\"numSamples\":10}");
}

TEST(Histogram, check1) {
  Histogram<int64_t> a;

  for (int64_t i = 5; i < 100000000000; i *= 10) {
    a = i;
  }

  std::ostringstream s;
  s << a;
  LOG(INFO) << a;
  EXPECT_EQ(s.str(), "{\"numSamples\":11,\"histogram\":[1,1,1,1,1,1,1,1,1,1,1,"
                     "0,0,0,0,0,0,0,0,0,0]}");
}

TEST(MinValue, check1) {
  MinValue<int64_t> a;

  for (int64_t i = -10; i < 10; i++) {
    a = i;
  }

  std::ostringstream s;
  s << a;
  EXPECT_EQ(s.str(), "-10");
}

TEST(MaxValue, check1) {
  MaxValue<int64_t> a;

  for (int64_t i = -10; i < 10; i++) {
    a = i;
  }

  std::ostringstream s;
  s << a;
  EXPECT_EQ(s.str(), "9");
}
