#include "FileDistributor.h"

#include <glog/logging.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <ftw.h> // ftw dir traversal
#include <unistd.h>

namespace gobjfs { 

static constexpr size_t MaxOpenFd = 100;

int32_t 
FileDistributor::createDirectories(const std::vector<std::string>& mountPoints, 
  size_t slots, 
  bool createFlag)
{
  slots_ = slots;

  size_t slotsToMake = slots/(mountPoints.size());

  int ret = 0;

  for (auto& mnt : mountPoints)
  {
    for (size_t idx = 0; idx < slotsToMake; idx ++)
    {
      auto dir = mnt + "/dir" + std::to_string(idx) + "/";
      if (createFlag) 
      {
        ret = mkdir(dir.c_str(), S_IRWXU | S_IRWXO | S_IRWXG);
        if (ret != 0) {
          LOG(ERROR) << "failed to create dir=" << dir << " errno=" << errno;
          break;
        }
      }
      dirs_.push_back(dir);
    }
    if (ret != 0) {
      LOG(ERROR) << "Failed to initialize mountpoint=" << mnt;
      break;
    }
  }

  if (ret == 0) {
    if (dirs_.size() != slots) {
      LOG(ERROR) << "dir size=" << dirs_.size() << " not equal to expected=" << slots;
      ret = -ENOTDIR;
    }
  }

  return ret;
}

const std::string&
FileDistributor::getDir(const std::string& fileName) const
{
  static std::hash<std::string> hasher;
  auto slot = hasher(fileName) % slots_;
  return dirs_.at(slot);
}

static int 
FileDeleterFunc(const char* fpath, 
  const struct stat* sb,
  int typeflag,
  struct FTW* ftwbuf)
{
  int ret = 0;
  if (typeflag == FTW_F) 
  {
    ret = ::unlink(fpath);
  }
  else if (typeflag == FTW_DP)
  {
    ret = ::rmdir(fpath);
    VLOG(2) << "deleting dir=" << fpath << std::endl;
  }
  if (ret == 0)
  {
    return FTW_CONTINUE; 
  }
  LOG(ERROR) << "stopping.  Failed to delete " << fpath << std::endl;
  return FTW_STOP;
}

int32_t 
FileDistributor::removeDirectories()
{
  const int numOpenFD = MaxOpenFd;
  int ret = 0;

  for (auto dir : dirs_)
  {
    // specified depth-first search FTW_DEPTH
    // because we can't delete dir unless
    // files in it are deleted
    ret |= nftw(dir.c_str(),
      FileDeleterFunc, 
      numOpenFD, 
      FTW_DEPTH | FTW_MOUNT | FTW_PHYS);
  
    if (ret == 0)
    {
      LOG(INFO) << "removed all dirs and files under path=" << dir;
    }
  }
  return ret;
}

}
