#!/bin/bash

for numthreads in 1 2 4 8 16 32 
do
  sed -i "s/max_threads=.*/max_threads=$numthreads/" ./bench_net_client.conf 
  ./BenchNetClient
done
