
#include <cstddef> // std::size_t
#include <gcommon.h>

static_assert(reinterpret_cast<std::size_t>(&(((gIOStatus *)0)->errorCode)) ==
                  8,
              "Error code not at 8 byte offset - Programs reading the "
              "gIOStatus from event pipe may need to be changed");

static_assert(sizeof(gIOStatus) == 16, "gIOstatus is no longer 16 bytes - "
                                       "Programs reading the gIOStatus from "
                                       "event pipe may need to be changed");
