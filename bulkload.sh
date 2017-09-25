#!/bin/bash

# The following script allows user to retrieve data and transactions
# and load the data into the database by running the following:
# bash bulkload.sh arg0

# The arguments can have the following values:
#		arg0: Number of clients - 10, 20, 40

# Conditions:
#       Project (Team10-Cassandra) is in home directory
#		cqlsh is within /temp/Cassandra/bin directory


# Bulk load data
cd /temp/apache-cassandra-3.11.0/bin
./cqlsh -f ~/Team10-Cassandra/schema.cql


cd ~/Team10-Cassandra/loadData
#echo -ne "\nLoading item data into Cassandra.."
# use python here to load data
#mvn -q install &>/dev/null
#mvn -q compile &>/dev/null
#mvn -q exec:java -Dexec.mainClass="database.Denormalize" -Dexec.args="$1"

echo -ne "Loading WAREHOUSE, DISTRICT, ORDER-LINE, STOCK-ITEM, PAYMENT_BY_CUSTOMER ITEM_BY_WAREHOUSE_DISTRICT and DELIVERY_BY_CUSTOMER data\n"

chmod +x *.py

# Load Warehouse
./LoadWarehouseData.py

# Load District
./LoadDistrictData.py

# Load StockItem
cqlsh -e "copy team10.stockitem (S_W_ID, S_I_ID, I_PRICE, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, S_DATA) from '~/Team10-Cassandra/data-files/stock.csv';"

# Update Stockitem
./LoadStockItemData.py

# Load OrderLine
cqlsh -e "copy team10.orderline (O_W_ID, O_D_ID, O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) from '~/Team10-Cassandra/data-files/order-line.csv' WITH NULL = 'null';"

# Update orderline
./LoadOrderLineData.py

# Load Payment_by_Customer
./LoadPaymentByCustomerData.py



# Load Item_by_Warehouse_District
copy team10.orderline (O_W_ID, O_D_ID, O_ID) TO 'order-line02.csv';

copy team10.item_by_warehouse_district (W_ID, D_ID, I_ID) from '~/Desktop/Team10-Cassandra/data-files/order-line02.csv';

./LoadItemByWarehouseDistrictData.py


