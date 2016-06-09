#pragma once 

#include <vector>
#include <string>

namespace gobjfs { 

/**
 * Filesytems slow down if you store millions of files
 * in a single directory.
 * This class written to prevent that.
 * Given a list of mountpoints, it creates subdirectories
 * and it hashes the filename to decide which subdir
 * the file should be stored/retrieved from
 */

class FileDistributor
{
  size_t slots_{0};
  std::vector<std::string> dirs_;

  public:

  static FileDistributor instance_;

  int32_t createDirectories(const std::vector<std::string> &mountPoints, 
    size_t slots, 
    bool createFlag);

  const std::string& getDir(const std::string& fileName) const;

  int32_t removeDirectories();
};

}
