#!/bin/bash

/bin/rm -f ./out

# Run this script from build/benchmark dir only

# the script expects a gioexecfile.conf.bak in local dir
# which doesnt contain any "cpu_core" line 
# because those lines are added to the file
# by this script

for ioexec in 1
do
	cp ./gioexecfile.conf.bak ./gioexecfile.conf
	for ((lines = 0; lines < ioexec; lines ++))
	do
		echo "cpu_core=$lines" >> ./gioexecfile.conf
	done
	cat ./gioexecfile.conf
		for objectsz in 4096 8192 16384 4194304
		do
		  sed -i "s/object_sz=.*/object_sz=$objectsz/" ./benchObjectFSWriter.conf
			echo $ioexec >> ./out
			sudo ./BenchObjectFSRW ./benchObjectFSWriter.conf >> ./out
      wait
      echo "finished ob size " $objectsz
		done
done
