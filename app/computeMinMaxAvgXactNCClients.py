#!/usr/bin/env python

import sys
import time
from decimal import *
from __future__ import division

throughputNum = []
filePath = 'throughput.txt';
with open(filePath, 'r+') as myFile:
	lines = myFile.readlines()

	for i in range(0, len(lines)):
		throughputNum = lines[i]

max_value = max(throughputNum)
min_value = min(throughputNum)
avg_value = sum(throughputNum)/len(throughputNum)

print "Minimum transaction outputs: %f" % min_value
print "Maximum transaction outputs: %f" % max_value
print "Average transaction outputs: %f" % avg_value
