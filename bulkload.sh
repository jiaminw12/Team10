#!/bin/bash

# The following script allows user to retrieve data and transactions
# and load the data into the database by running the following:
# bash bulkload.sh arg0

# The arguments can have the following values:
#		arg0: Number of clients - 10, 20, 40

# Conditions:
#       Project (Team10-Cassandra) is in home directory
#		cqlsh is within /temp/Cassandra/bin directory

declare -r DATA_FOLDER="data-files"
declare -r XACT_FOLDER="xact-files"

# Create data folder only if not exist
echo -ne "Checking whether data-files and xact-files exist..."
if [ -d $DATA_FOLDER && -d XACT_FOLDER]
then
    echo "yes"
else
    echo "Start downloading data-files and xact-files..."
    wget http://www.comp.nus.edu.sg/~cs4224/4224-project-files.zip
    unzip 4224-project-files.zip
    echo "data-files and xact-files are created successfully\n"
fi


# Bulk load data

cd /temp/Cassandra/bin
./cqlsh -f ~/Team10-Cassandra/schema.cql


cd ~/Team10-Cassandra/loadData
#echo -ne "\nLoading item data into Cassandra.."
# use python here to load data
#mvn -q install &>/dev/null
#mvn -q compile &>/dev/null
#mvn -q exec:java -Dexec.mainClass="database.Denormalize" -Dexec.args="$1"

echo -ne "Loading WAREHOUSE, DISTRICT, ORDER-LINE, STOCK-ITEM, PAYMENT_BY_CUSTOMER ITEM_BY_WAREHOUSE_DISTRICT and DELIVERY_BY_CUSTOMER data\n"

chmod +x *.py

./LoadWarehouseData.py
./LoadDistrictData.py


copy team10.stockitem (S_W_ID, S_I_ID, I_PRICE, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, S_DATA) from '~/Team10-Cassandra/data-files/stock.csv';

# update stockitem
./LoadStockItemData.py

copy team10.orderline (O_W_ID, O_D_ID, O_ID, OL_NUMBER, OL_I_ID, O_ENTRY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) from '~/Team10-Cassandra/data-files/order-line.csv' WITH NULL = 'null';

# update orderline
./LoadOrderLineData.py




