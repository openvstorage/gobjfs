#!/bin/bash

for numprocs in 1 2 3 4
do
    echo "numpro" $numprocs
for numthreads in 1 2 4 8 16 32 
do
  sed -i "s/max_threads=.*/max_threads=$numthreads/" ./bench_net_client.conf 
  for (( c = 1; c <= $numprocs; c++))
  do
    echo "num_threads="$numthreads "num_process="$c
    ./BenchNetClient&
  done
  wait
done
done
