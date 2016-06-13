SATA SECTION
=====================
only_sata_pureread.ioexecfile.out
contains run with pure reads on 9 SATA SSDs

============
1_1000_only_sata_nosync.ioexecfile.out

contains run with write percent of 1:1000 on 9 SATA SSDs
without O_SYNC flag used in benchmark

1:1000

===================

1_1000_only_sata_sync.ioexecfile.out

contains run with write percent of 1:1000 on 9 SATA SSDs

1:1000

===================
1_2000_only_sata_nosync.csv

contains run with write percent of 1:1000 on 9 SATA SSDs

no O_SYNC


===================

1_2000_only_sata_sync.ioexecfile.out

the readWriteRatio variable was changed from (0, 1000) to (0, 2000) in BenchIOExecFile.cpp

This hack to change "readWriteRatio" in BenchIOExecFile.cpp is not required
Now its a config parameter called "total_read_write_scale"

====================
NVME SECTION
===================

only_nvme_pureread.csv

contains run where only NVME SSD /dev/nvme0n1 was used in benchmark 
only file reads done

1_2000_with_nvme.ioexecfile.csv

contains run where NVME SSD /dev/nvme0n1 was also deployed in benchmark 
in addition to the 9 SATA SSDs


====================
1_2000_only_nvme_sync.csv

contains run where only NVME SSD /dev/nvme0n1 was used in benchmark 
file writes done using O_SYNC

1:2000

====================
1_2000_only_nvme_nosync.csv

contains run where only NVME SSD /dev/nvme0n1 was used in benchmark 
file writes done without O_SYNC

1:2000

===============

1_1000_only_nvme_sync.csv

contains run where only NVME SSD /dev/nvme0n1 was used in benchmark 
file writes done with O_SYNC

1:1000 write:read ratio

===============

1_1000_only_nvme_nosync.csv

contains run where only NVME SSD /dev/nvme0n1 was used in benchmark 
file writes without O_SYNC

1:1000 write:read ratio

===================

NVME + SATA COMBO

separate_nvme.ioexecfile.csv
separate_sata.ioexecfile.csv

contains run where two benchmarks were run side-by-side
one on NVME
one on SATA

1:2000 with O_SYNC
