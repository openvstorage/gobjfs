
After successful make, the "build/benchmark" directory will have the 
benchmark executables to be run.

=================================================

To run BenchIOExecFile, you need 2 config files in the current directory

1) benchioexec.conf - this provides benchmark options like how many threads 
to run or how many operations to do.  A sample be found in "benchmark" dir

2) gioexecfile.conf - this provides IOExecutor options like what queue 
depth to maintain.  A sample can be found in "src" dir.

=================================================

To run BenchObjectFSRW, you need to provide a config file on the 
command line.  

$ ./BenchObjectFSRW <config_file>

A sample file can be found in "benchmark/benchObjectFSWriter.conf"

This file contains an option called "ioconfig_file=".
This should be set to the IOExecutor config file, a sample of which can
be found in "src/gioexecfile.conf"
