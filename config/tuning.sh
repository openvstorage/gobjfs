# This file lists various config parameters which should be 
# examined (and maybe changed) in a production or benchmark

# References
# https://www.kernel.org/doc/Documentation/block/queue-sysfs.txt

# ======================================================
# SSD Interrupts should go to the requesting CPU core

# For SATA, find interrupt number for the SSD
# Its the leftmost column in "cat /proc/interrupts"
# and set the smp_affinity to distribute to the relevant cores
sudo sh -c "echo f > /proc/irq/<number>/smp_affinity"

# If you are using NVME SSD, check 
cat /sys/block/nvme0n1/queue/rq_affinity
# this file should say "1" or "2"

# ======================================================
# Change SSD read-ahead size
sudo sh -c "echo 16 > /sys/block/nvme0n1/queue/read_ahead_kb"

# ======================================================

# If you want to drop filesystem caches, first sync
sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

# check the kernel caches
cat /proc/slabinfo

# ======================================================
# Set SSD scheduler to noop 

# for SATA SSDs
sudo sh -c 'echo noop > /sys/block/sdc/queue/scheduler'
sudo sh -c 'echo noop > /sys/block/sdb/queue/scheduler'

# For NVME SSDs, its set to none by default.  Check
sudo cat /sys/block/nvme0n1/queue/scheduler

# Also check "nomerges" is not 0
cat /sys/block/nvme0n1/queue/nomerges

# ========================================================
# Doing Async IO? 
# increase the number of outstanding IO
sudo sysctl -w fs.aio-max-nr=131072

# ========================================================

# Opening large number of files? 
# increase the system wide limit
sudo sysctl -w fs.file-max=16384

# To increase per-user file limits
vi /etc/security/limits.conf
# and add lines
# "<username> nofile soft <max-open-files>"
# "<username> nofile hard <max-open-files>"

# Changing limits.conf will not take effect without PAM changes
# add line "session required pam_limits.so" to following files
vi /etc/pam.d/common-session
vi /etc/pam.d/common-session-noninteractive
# ========================================================
# Directory mounting

# Mount with no access time or dir access time updates
mount -o noatime,nodiratime /dev/nvme0n1 <fspath>

# u can also add dir sync to keep dir metadata safe
mount -o dirsync

# ========================================================
# Networking ? Increase TCP buffers

sudo sh -c 'echo 262144 > /proc/sys/net/core/rmem_default'
sudo sh -c 'echo 262144 > /proc/sys/net/core/wmem_default'
sudo sh -c 'echo 262144 > /proc/sys/net/core/rmem_max'
sudo sh -c 'echo 262144 > /proc/sys/net/core/wmem_max'

# ========================================================
# Increase MTU to do jumbo frames
sudo ifconfig eth0 mtu 9000

# ========================================================


