#!/usr/bin/env bash

# This sample bash script (using cut, paste, sort and join commands) generates a csv file w.r.t. a desired schema.
# The example here performs a left-outerjoin of Customer and Order relations, and projects out the following 4 attributes: 
# customer name (4th, 5th and 6th attributes of Customer),  and order id (3rd attribute of Order).
# The join columns in Customer/Order are its first three attributes.
# The output is written to the file tmp-custorder.csv.
# 
# Here's a brief explanation of the commands.
# 
# 1) cut -d',' --output-delimiter=- -f1,2,3 customer.csv
# This produces a single-column output formed by concatenating the first three columns of customer.csv using delimiter "-".
#
# 2) cut -d',' --output-delimiter=- -f1,2,3 order.csv
# This is similar to command #1 for order.csv.
#
# 3) paste -d',' <(cut -d',' --output-delimiter=- -f1,2,3 customer.csv) customer.csv | sort -t',' -k1,1
# This outputs a modified version of customer.csv that has an additional first column (created by command #1). The output is sorted by the new column. The sorting is necessary as we are going to do a sort-merge join  of Customer and Order using this output.
#
# 4) paste -d',' <(cut -d',' --output-delimiter=- -f1,2,3 order.csv) order.csv | sort -t',' -k1,1
# This is similar to command #3 for order.csv. The output is again sorted on the newly created first column that is concatenated from the first three columns of order.csv.
#
# 5) join -a 1 -j 1 -t ',' -o 1.5 1.6 1.7 2.4 -e "null" <(....) <(....)
# This performs a left-outerjoin of the sorted outputs from commands #3 and #4. The output consists of columns 5,6, and 7 (from command #3) and column 4 (from command #4).

join -a 1 -j 1 -t ',' -o 1.5 1.6 1.7 2.4 -e "null" <(paste -d',' <(cut -d',' --output-delimiter=- -f1,2,3 customer.csv) customer.csv | sort -t',' -k1,1) <(paste -d',' <(cut -d',' --output-delimiter=- -f1,2,3 order.csv) order.csv | sort -t',' -k1,1) > tmp-custorder.csv

