#!/usr/bin/env bash

# This script uses the sed command to replace all null values with UNKNOWN in all csv files.

for f in *.csv
do
	sed -i -e 's/,null,/,UNKNOWN,/g' -e 's/^null,/UNKNOWN,/' -e 's/UNKNOWN,null,/UNKNOWN,UNKNOWN,/g' -e 's/,null$/,UNKNOWN/' $f
done

