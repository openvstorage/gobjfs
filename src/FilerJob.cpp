#include <FilerJob.h>
#include <gobjfs_log.h>
#include <unistd.h>
#include <util/os_utils.h>

namespace gobjfs {

using gobjfs::os::FD_INVALID;
using gobjfs::os::IsDirectIOAligned;
using gobjfs::os::RoundToNext512;

std::ostream &operator<<(std::ostream &os, FileOp op) {
  switch (op) {
  case FileOp::Nop:
    os << "Nop";
    break;
  case FileOp::Write:
    os << "Write";
    break;
  case FileOp::Read:
    os << "Read";
    break;
  case FileOp::Sync:
    os << "Sync";
    break;
  case FileOp::Delete:
    os << "Delete";
    break;
  case FileOp::NonAlignedWrite:
    os << "NonAlignedWrite";
    break;
  default:
    os << "Unknown";
    break;
  }
  return os;
}

FilerJob::FilerJob(const char *filename, FileOp op)
    : op_(op), fileName_(filename) {
  assert(waitTime() == 0);
  assert(serviceTime() == 0);
  assert(filename != nullptr);
}

FilerJob::FilerJob(const int fd, FileOp op) : op_(op), fd_(fd) {
  assert(waitTime() == 0);
  assert(serviceTime() == 0);
  assert(fd != FD_INVALID);
}

FilerJob::~FilerJob() {}

int32_t FilerJob::prepareCallblock(iocb *cb) {
  if (this->op_ == FileOp::Write) {

    io_prep_pwrite(cb, this->fd_, this->buffer_, this->size_, this->offset_);

  } else if (this->op_ == FileOp::Read) {

    io_prep_pread(cb, this->fd_, this->buffer_, this->size_, this->offset_);

  } else {

    assert("which op" == 0);
  }

  cb->data = this;
  this->setWaitTime();

  VLOG(1) << " job=" << (void *)this << " op=" << this->op_
          << " fd=" << this->fd_ << " offset=" << this->offset_
          << " size=" << this->size_;

  return 0;
}

bool FilerJob::isValid(std::ostringstream &ostr) {
  bool isValid = true;

  if ((op_ != FileOp::Read) && (op_ != FileOp::Write))
  {
    return isValid;
  }

  if (!gobjfs::os::IsFdOpen(fd_)) {
    ostr << ":fd=" << fd_ << " has errno=" << errno;
    isValid = false;
  }
  if (IsDirectIOAligned(offset_)) {
    ostr << ":offset=" << offset_ << " is not 512 aligned";
    isValid = false;
  }
  if (IsDirectIOAligned((uint64_t)buffer_)) {
    ostr << ":buffer=" << buffer_ << " is not 512 aligned";
    isValid = false;
  }
  if (IsDirectIOAligned(size_)) {
    ostr << ":size=" << size_ << " is not 512 aligned";
    isValid = false;
  }

  return isValid;
}


void FilerJob::setBuffer(off_t fileOffset, char *buffer, size_t size) {
  buffer_ = buffer;
  offset_ = fileOffset;
  userSize_ = size;
  size_ = RoundToNext512(userSize_);
}

void FilerJob::reset() {
  VLOG(2) << "FilerJob reset "
          << ":completionFd=" << completionFd_
          << ":completionId=" << completionId_;

  gIOStatus iostatus;
  iostatus.completionId = completionId_;
  iostatus.errorCode = retcode_;

  if (write(completionFd_, &iostatus, sizeof(iostatus)) != sizeof(iostatus)) {
    LOG(ERROR) << "For job=" << (void *)this
               << " Failed to signal IO status for "
               << " completionId: " << completionId_
               << " returnCode: " << retcode_;
  }

  setServiceTime();
}
}
