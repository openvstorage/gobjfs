#include <gobjfs_log.h>
#include <gtest/gtest.h>

int main(int argc, char **argv) {
  // google::InitGoogleLogging(argv[0]); TODO logging
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
