#!/bin/bash

if [ $# -ne 1 ]; then
  echo "supply run identifier as first arg"
  exit 1
fi

for numprocs in 8 16 32 1 2 4
do
  for (( c = 1; c <= $numprocs; c++))
  do
    ./EdgeProcess $1 $numprocs &
  done
  wait
done
