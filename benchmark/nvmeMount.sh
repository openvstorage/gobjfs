# used for BenchIOExecFile
sudo umount /mnt
sudo mkfs.xfs -f /dev/nvme0n1
sudo mount -o discard,attr2,noquota,inode64,noatime /dev/nvme0n1 /mnt
sudo chmod o+w /mnt
#for i in {1..1024}; do mkdir /mnt/dir$i; done
