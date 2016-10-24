#!/usr/bin/python

''' 
Test data file generator program

Writes file number and offset in each 4K block
This can be used by benchmark client to verify if it has indeed 
read the block corresponding to the request

This is sample C code which can be used to verify a block
{
  std::string fileString(buf, 8); 
  std::string offsetString(buf + 8, 8); 
  assert(atoll(fileString.c_str()) == file_number);
  assert(atoll(offsetString.c_str()) == offset);
}

'''

import sys

def main(argv):

  if (len(sys.argv) != 3) :
    print "usage <numfiles> <numblocks>"
    sys.exit(1)

  numfiles = int(sys.argv[1]) 
  numblocks = int(sys.argv[2]) 

  for file in xrange(0, numfiles):
    filename = str(file) + ".txt"
    with open(filename, "wb") as out:
      for off in xrange(0, numblocks):
        out.seek(off * 4096)
        out.write(str(file).zfill(8) + str(off).zfill(8));

if __name__ == "__main__":
  main(sys.argv[1:])
