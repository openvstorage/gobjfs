# used for BenchIOExecFile

sudo chmod o+w /mnt

for dir in b c d e f g h i j k l m n o p q r s t u v w
do 

mountpoint=/mnt/sd${dir}
sudo umount ${mountpoint}

devname=/dev/sd${dir}
echo $devname
sudo mkfs.xfs -f $devname

mkdir $mountpoint

sudo mount -o noquota,attr2,inode64,noatime,discard $devname $mountpoint
sudo chmod o+w $mountpoint

done
