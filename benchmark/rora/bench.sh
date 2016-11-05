#!/bin/bash

for numprocs in 8 16 32 1 2 4
do
  for (( c = 1; c <= $numprocs; c++))
  do
    ./EdgeProcess async1 $numprocs &
  done
  wait
done
