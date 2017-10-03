#!/bin/bash

# The following script allows user to benchmark Cassandra performance
# Enter arguments to select the data to benchmark:
# bash benchmark.sh arg0

# arg0 can have the following values:
#		arg0: Number of clients - 10, 20, 40


# Run app
rm -rf log
mkdir log
cd ~/Team02/app
chmod +x *.py
echo -ne "Running Performance Measurement now ... \n"

echo -ne "Execute $1 clients ...\n"
let "NUM_CLIENTS = $1"

for i in `seq 1 $NUM_CLIENTS`; do
	./MainApp.py "$i".txt 1> ~/Team02/log/output$i.log 2> ~/Team02/log/error$i.log &
done
wait
echo "completed"

./computeMinMaxAvgXactNCClients.py

rm throughput.txt

# Part 4
cqlsh -e "SELECT sum(W_YTD) FROM team02.Warehouse;"
cqlsh -e "SELECT sum(D_YTD), sum(D_NEXT_O_ID) FROM team02.District;"
cqlsh -e "SELECT sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT) FROM team02.payment_by_customer;"
cqlsh -e "SELECT max(O_ID), sum(O_OL_CNT) FROM team02.orderline;"
cqlsh -e "SELECT sum(OL_AMOUNT), sum(OL_QUANTITY) FROM team02.orderline;"
cqlsh -e "SELECT sum(S_QUANTITY) FROM team02.item_by_warehouse_district;"
cqlsh -e "SELECT sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) FROM team02.stockitem;"
