#pragma once

namespace gobjfs {
namespace xio {

static constexpr int MAX_PORTAL_THREADS = 16;

static constexpr int POLLING_TIME_USEC_DEFAULT = 0;
// From accelio manual
// polling_timeout_us: Defines how much to do receive-side-polling before yielding the CPU 
// and entering the wait/sleep mode. When the application requires latency over IOPs and 
// willing to poll on CPU, setting polling timeout to ~15-~70 us will decrease the latency 
// substantially, but will increase CPU cycle by consuming more power (watts).
// http://www.accelio.org/wp-admin/accelio_doc/index.html

}
}
