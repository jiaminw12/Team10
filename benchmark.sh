#!/bin/bash

# The following script allows user to benchmark Cassandra performance
# Enter arguments to select the data to benchmark:
# bash benchmark.sh arg0

# arg0 can have the following values:
#		arg0: Number of clients - 10, 20, 40


# Run app
rm -rf log
mkdir log
cd ~/Team10/app
chmod +x *.py
echo -ne "Running Performance Measurement now ... \n"

echo -ne "Execute $1 clients ...\n"
let "NUM_CLIENTS = $1"

for i in `seq 1 $NUM_CLIENTS`; do
	./MainApp.py "$i".txt 1> ~/Team10/log/output$i.log 2> ~/Team10/log/error$i.log &
done
wait
echo "completed"

./computeMinMaxAvgXactNCClients.py

rm ~/Team10/app/throughput.txt

# Part 4
echo -ne "1. SELECT sum(W_YTD) FROM team10.Warehouse ... \n"
cqlsh -e "SELECT sum(W_YTD) FROM team10.Warehouse;"

echo -ne "2. SELECT sum(D_YTD), sum(D_NEXT_O_ID) FROM team10.District ... \n"
cqlsh -e "SELECT sum(D_YTD), sum(D_NEXT_O_ID) FROM team10.District;"

echo -ne "3. SELECT sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT) FROM team10.payment_by_customer ... \n"
cqlsh -e "SELECT sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT) FROM team10.payment_by_customer;"

echo -ne "4. SELECT max(O_ID), sum(O_OL_CNT) FROM team10.orderline ... \n"
cqlsh -e "SELECT max(O_ID), sum(O_OL_CNT) FROM team10.orderline;"

echo -ne "5. SELECT sum(OL_AMOUNT), sum(OL_QUANTITY) FROM team10.delivery_by_customer ... \n"
cqlsh -e "SELECT sum(OL_AMOUNT), sum(OL_QUANTITY) FROM team10.delivery_by_customer;"

echo -ne "6. SELECT sum(S_QUANTITY) FROM team10.item_by_warehouse_district ... \n"
cqlsh -e "SELECT sum(S_QUANTITY) FROM team10.item_by_warehouse_district;"

echo -ne "7. SELECT sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) FROM team10.stockitem ... \n"
cqlsh -e "SELECT sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) FROM team10.stockitem;"
