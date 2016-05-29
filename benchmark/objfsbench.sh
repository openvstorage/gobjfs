#!/bin/bash

/bin/rm -f ./objfsbench.out

# Run this script from build/benchmark dir only

# the script expects a gioexecfile.conf.bak in local dir
# which doesnt contain any "cpu_core" line 
# because those lines are added to the file
# by this script

for ioexec in 1 2 4 8 16
do
   cp ./gioexecfile.conf.bak ./gioexecfile.conf
   for ((lines = 0; lines < ioexec; lines ++))
   do
      echo "cpu_core=$lines" >> ./gioexecfile.conf
   done
   cat ./gioexecfile.conf
   for per_core_reader in 1 2 4
   do
      reader_threads=$(($per_core_reader * $ioexec))

      for objectsz in 4096  8192 16384 4194304
      do
        sed -i "s/max_reader_threads=.*/max_reader_threads=$reader_threads/" ./benchObjectFSWriter.conf
        sed -i "s/object_sz=.*/object_sz=$objectsz/" ./benchObjectFSWriter.conf
         echo $ioexec >> ./objfsbench.out
         sudo ./BenchObjectFSRW ./benchObjectFSWriter.conf >> ./objfsbench.out
      done
   done
done
