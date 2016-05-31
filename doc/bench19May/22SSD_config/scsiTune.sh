#!/bin/bash
# run this as "sudo <scriptname>"

for dir in b c d e f g h i j k l m n o p q r s t u v w
do
# turn off scheduler 
  echo noop > /sys/block/sd${dir}/queue/scheduler

# random IO, no need to attempt merge
  echo 2 > /sys/block/sd${dir}/queue/nomerges

# increase the max_sectors per request in block device layer
  cat /sys/block/sd${dir}/queue/max_hw_sectors_kb > /sys/block/sd${dir}/queue/max_sectors_kb

# we will be doing random IO - no need to fetch more than 16K
  echo 16 > /sys/block/sd${dir}/queue/read_ahead_kb
done
