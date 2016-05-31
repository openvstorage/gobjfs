
# Open vStorage Ganesha Object FileSystem (GObjFS)
The Open vStorage Ganesha Object File System (GObjFS) provides a flat object store to an ASD (one raw block device). It was designed to be used with [Alba](https://github.com/openvstorage/alba)/[Open vStorage](https://github.com/openvstorage/home). The performance characteristic are hence tuned to be:
* Writes - High Throughput (high latency is fine as writes are in large chunks and where acknowledged to the application while in the write buffer)
* Reads - Low Latency and high throughput


It support the following functions:
* Get (ContainerID + Offset + Length)
* Put (Payload and return ContainerID+Offset) - no append
* Delete (ContainerID+Offset+Length)

At the most basic level this component divides an SSD into logical containers (256MB each).

This component will use the following:

* The kernel block device
* Block Allocation Bitmap (per container) - In memory structure where a bit (for every 16K) represents whether the block is used or empty. This BAB doesn’t have to be persistent across reboots.
* Each container will have a write lock
* There will be a containerID -> actual region on disk mapping

# License
GObjFS is licensed under the [GNU Affero General Public License v3](http://www.gnu.org/licenses/agpl-3.0.html).


## File a bug
Open vStorage and its automation is quality checked to the highest level.
Unfortunately we might have overlooked some tiny topics here or there.
The Open vStorage GObjFS Project maintains a [public issue tracker](https://github.com/openvstorage/gobjfs/issues)
where you can report bugs and request features.
This issue tracker is not a customer support forum but an error, flaw, failure, or fault in the Open vStorage software.
