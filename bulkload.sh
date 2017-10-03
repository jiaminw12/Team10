#!/bin/bash

# The following script allows user to retrieve data and transactions
# and load the data into the database by running the following:
# bash bulkload.sh

# Conditions:
#       Project (Team10-Cassandra) is in home directory
#		cqlsh is within /temp/Cassandra/bin directory

# Bulk load data
cd /temp/apache-cassandra-3.11.0/bin
./cqlsh -f ~/Team10/schema.cql

echo -ne "Loading WAREHOUSE, DISTRICT, ORDER-LINE, STOCK-ITEM, PAYMENT_BY_CUSTOMER ITEM_BY_WAREHOUSE_DISTRICT and DELIVERY_BY_CUSTOMER data\n"

cd ~/Team10/loadData
chmod +x *.py

# Load Warehouse
./LoadWarehouseData.py

# Load District
./LoadDistrictData.py

# Load StockItem
cqlsh -e "copy team10.stockitem (S_W_ID, S_I_ID, I_PRICE, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, S_DATA) from '~/Team10/data-files/stock.csv';"

# Update Stockitem
./LoadStockItemData.py

# Load Payment_by_Customer
./LoadPaymentByCustomerData.py

cqlsh -e "copy team10.orderline (O_W_ID, O_D_ID, O_ID, OL_NUMBER, OL_I_ID, O_ENTRY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) from '~/Team10/data-files/order-line.csv' WITH NULL = 'null';";

# Update orderline
./LoadOrderLineData.py

# create a dummy csv
cp ~/Team10/data-files/warehouse.csv ~/Team10/data-files/order-line02.csv
cp ~/Team10/data-files/warehouse.csv ~/Team10/data-files/order-line03.csv

cqlsh -e "copy team10.orderline (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_ENTRY_D, OL_NUMBER, OL_I_ID, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY) TO '/home/stuproj/cs4224j/Team10/data-files/order-line02.csv';"

# Load Delivery_by_Customer
cqlsh -e "copy team10.delivery_by_customer (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_ENTRY_D, OL_NUMBER, OL_I_ID, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY) from '/home/stuproj/cs4224j/Team10/data-files/order-line02.csv';"

./LoadDeliveryByCustomerData.py

# Load Item_by_Warehouse_District
cqlsh -e "copy team10.orderline (O_W_ID, O_D_ID, O_ID) TO '/home/stuproj/cs4224j/Team10/data-files/order-line03.csv';"

cqlsh -e "copy team10.item_by_warehouse_district (W_ID, D_ID, I_ID) from '/home/stuproj/cs4224j/Team10/data-files/order-line03.csv'"

./LoadItemByWarehouseDistrictData.py



