#pragma once

namespace gobjfs {
namespace xio {

// number of requests which will be batched in one aio_readv
static constexpr int32_t MAX_AIO_BATCH_SIZE = 50;

// right now, no way to figure out the msgpack header size
// based on number of requests being sent
// ideally, we would like inline_header size to be 
// some formula based on max aio batch size
static constexpr int32_t MAX_INLINE_HEADER_OR_DATA = 8 * 4096;

static constexpr int POLLING_TIME_USEC_DEFAULT = 0;
// From accelio manual
// polling_timeout_us: Defines how much to do receive-side-polling before yielding the CPU 
// and entering the wait/sleep mode. When the application requires latency over IOPs and 
// willing to poll on CPU, setting polling timeout to ~15-~70 us will decrease the latency 
// substantially, but will increase CPU cycle by consuming more power (watts).
// http://www.accelio.org/wp-admin/accelio_doc/index.html

}
}
