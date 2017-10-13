#!/bin/bash

# The following script allows user to benchmark Cassandra performance
# Enter arguments to select the data to benchmark:
# bash benchmark.sh arg0

# arg0 can have the following values:
#		arg0: Number of clients - 10, 20, 40


# Run app

#readarray lines < nodeList.txt
IFS=$'\n' read -d '' -r -a lines < nodeList.txt

rm -rf log
mkdir log
cd ~/Team10/app
chmod +x *.py
echo -ne "Running Performance Measurement now ... \n"

echo -ne "Execute $1 clients ...\n"
let "CONSISTENCY_LEVEL = $1"

for i in 5 10 15 20; do
	echo -ne "./MainApp.py $i.txt $CONSISTENCY_LEVEL ... \n"
	./MainApp.py "$i".txt "$CONSISTENCY_LEVEL" "${lines[0]}" 1> ~/Team10/log/output$i.log 2> ~/Team10/log/error$i.log &
done

for i in 1 6 11 16; do
	echo -ne "./MainApp.py $i.txt $CONSISTENCY_LEVEL ... \n"
	./MainApp.py "$i".txt "$CONSISTENCY_LEVEL" "${lines[1]}" 1> ~/Team10/log/output$i.log 2> ~/Team10/log/error$i.log &
done

for i in 2 7 12 17; do
	echo -ne "./MainApp.py $i.txt $CONSISTENCY_LEVEL ... \n"
	./MainApp.py "$i".txt "$CONSISTENCY_LEVEL" "${lines[2]}" 1> ~/Team10/log/output$i.log 2> ~/Team10/log/error$i.log &
done

for i in 3 8 13 18; do
	echo -ne "./MainApp.py $i.txt $CONSISTENCY_LEVEL ... \n"
	./MainApp.py "$i".txt "$CONSISTENCY_LEVEL" "${lines[3]}" 1> ~/Team10/log/output$i.log 2> ~/Team10/log/error$i.log &
done

for i in 4 9 14 19; do
	echo -ne "./MainApp.py $i.txt $CONSISTENCY_LEVEL ... \n"
	./MainApp.py "$i".txt "$CONSISTENCY_LEVEL" "${lines[4]}" 1> ~/Team10/log/output$i.log 2> ~/Team10/log/error$i.log &
done
wait

./computeMinMaxAvgXactNCClients.py

rm ~/Team10/app/throughput.txt

# Part 4
echo -ne "\n1. SELECT sum(W_YTD) FROM team10.Warehouse ... \n"
cqlsh ${lines[0]} -e "SELECT sum(W_YTD) FROM team10.Warehouse;"

echo -ne "2. SELECT sum(D_YTD), sum(D_NEXT_O_ID) FROM team10.District ... \n"
cqlsh ${lines[0]} -e "SELECT sum(D_YTD), sum(D_NEXT_O_ID) FROM team10.District;"

echo -ne "3. SELECT sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT) FROM team10.payment_by_customer ... \n"
cqlsh ${lines[0]} -e "SELECT sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT) FROM team10.payment_by_customer;"

echo -ne "4. SELECT max(O_ID), sum(O_OL_CNT) FROM team10.order_by_asc ... \n"
cqlsh ${lines[0]} -e "SELECT max(O_ID), sum(O_OL_CNT) FROM team10.order_by_asc;"

echo -ne "5. SELECT sum(OL_AMOUNT), sum(OL_QUANTITY) FROM team10.orderline ... \n"
cqlsh ${lines[0]} -e "SELECT sum(OL_AMOUNT), sum(OL_QUANTITY) FROM team10.orderline;"

echo -ne "6. SELECT sum(S_QUANTITY) FROM team10.item_by_warehouse_district ... \n"
cqlsh ${lines[0]} -e "SELECT sum(S_QUANTITY) FROM team10.stockitem;"

echo -ne "7. SELECT sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) FROM team10.stockitem ... \n"
cqlsh ${lines[0]} -e "SELECT sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) FROM team10.stockitem;"

exit
