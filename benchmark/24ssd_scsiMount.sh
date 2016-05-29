# used for BenchIOExecFile

function makeMount()
{
  mountpoint=/mnt/sd${1}
  devname=/dev/sd${1}

  sudo umount ${mountpoint}  
  sudo mkfs.xfs -f $devname
  mkdir $mountpoint
  sudo mount -o noquota,attr2,inode64,noatime,discard $devname $mountpoint
  sudo chmod o+w $mountpoint
}

sudo chmod o+w /mnt

sudo umount /mnt/sdy
sudo mkfs.xfs -f /dev/fioa
mkdir /mnt/sdy
sudo mount -o noquota,attr2,inode64,noatime,discard /dev/fioa /mnt/sdy
sudo chmod o+w /mnt/sdy

sudo umount /mnt/sdz
sudo mkfs.xfs -f /dev/nvme0n1
mkdir /mnt/sdz
sudo mount -o noquota,attr2,inode64,noatime,discard /dev/nvme0n1 /mnt/sdz
sudo chmod o+w /mnt/sdz

for dir in b c d e f g h i j k l m n o p q r s t u v w 
do 
# run mkfs in parallel
  (makeMount ${dir}) &
done

# wait for all mkfs to end
wait 

