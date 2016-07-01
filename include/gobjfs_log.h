#pragma once
#include <boost/log/trivial.hpp>

#define LOG(level) LOG_##level
#define LOG_IF(level, cond)                                                    \
  if (cond)                                                                    \
  LOG_##level

#define LOG_DEBUG BOOST_LOG_TRIVIAL(debug)
#define LOG_INFO BOOST_LOG_TRIVIAL(info)
#define LOG_WARNING BOOST_LOG_TRIVIAL(warning)
#define LOG_ERROR BOOST_LOG_TRIVIAL(error)
#define LOG_FATAL BOOST_LOG_TRIVIAL(fatal)

// TODO logging Need to reimplement glog functionality here
#define VLOG(level) BOOST_LOG_TRIVIAL(debug)

// TODO logging Need to reimplement glog functionality here
#define LOG_EVERY_N(level, number) LOG_##level
