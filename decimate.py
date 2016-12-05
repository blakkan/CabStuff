#! /usr/bin/python
#
# Script to decimate a csv file.
#
# Argument is a flot, the decimal proportion of lines to retain.
#
# this is a probabalistic filter, not a "comb"
#
# Second argument, if present, is a seed for the random number generator
#
# to make it deterministic

import sys
import random

if len(sys.argv) < 2 or sys.argv[1] == '?' or len(sys.argv) > 3:
	print ("usage:  %s <float> [<int>] where <float> is the decimal portion to (probabalistically) retain" % sys.argv[0])
	print ("and optional argument <int> is an integer seed for random number generation")
	print ("takes data to decimate from stdin, sends reduced data to stdout")
	exit()


proportion = float(sys.argv[1])

if len(sys.argv) == 3:
	random.seed(int(sys.argv[2]))


for line in sys.stdin:
	if random.random() < proportion:
		print(line,)
