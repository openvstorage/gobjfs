sample program to test if readers can read consistent copy while writer is active on same db

the only error seen are during DB::OpenForReadOnly()
it fails sometimes due to missing logs.
sample error shown below

E1024 13:03:17.958070 12823 read_example.cc:60] failed to open db.  err=IO error: /tmp/rocksdb_simple_example/000136.log: No such file or directory
