
In the paper 
Chen, F., Lee, R., Zhang, X.: Essential Roles of Exploiting Parallelism of Flash Memory based Solid State Drives in High-Speed Data Processing. In: HPCA, pp. 266–277 (2011)

Section 5.2 states that mixing reads and writes can decrease
the IOPS provided by the SSD

from the paper "when running random reads and writes
together, we see a strong negative impact. For example,
sequential writes can achieve a bandwidth of 61.4MB/sec
when running individually, however when running with
random reads, the bandwidth drops by a factor of 4.5 to
13.4MB/sec. Meanwhile, the bandwidth of random reads
also drops from 21.3MB/sec to 19.4MB/sec"

The spreadsheet in this directory replicates their result
for random read and writes of 4K done using fio.

The command line is given in the spreadsheet

This benchmark was run on two machines in Belgium

1) OvH1 Linux kernel 4.2.0-35 10-SSD (178.33.63.210) 
NVME and SATA portion run here

2) 24-SSD (10.100.184.253)
Fusion-IO run here

