#!/bin/bash

/bin/rm -f ./ioexecfile.csv

export GLOG_log_dir=/home/sandeep/glog_hist
# Run this script from build/benchmark dir only

# the script expects a gioexecfile.conf.bak in local dir
# which doesnt contain any "cpu_core" line 
# because those lines are added to the file
# by this script
sed -i "s/new_instance=.*/new_instance=false/" ./benchioexec.conf

for ioexec in 1 2 4 8 16
do
	cp ./gioexecfile.conf.bak ./gioexecfile.conf
	for ((lines = 0; lines < ioexec; lines ++))
	do
		echo "cpu_core=$lines" >> ./gioexecfile.conf
	done
	for per_core_reader in 1 2 
	do
		reader_threads=$(($per_core_reader * $ioexec))

    sed -i "s/max_threads=.*/max_threads=$reader_threads/" ./benchioexec.conf
    ./BenchIOExecFile >> ./ioexecfile.csv
	done
done
