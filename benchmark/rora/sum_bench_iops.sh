
for i in 1 2 4 8 16 32
do
  grep num_edges=$i: EdgeProcess*log |cut -d: -f9 | cut -d= -f2 | python -c "import sys; print sum(int(l) for l in sys.stdin)"
done
