#!/bin/bash

# The following script allows user to retrieve data and transactions
# and load the data into the database by running the following:
# bash bulkload.sh

# Conditions:
#       Project (Team10) is in home directory
#		cqlsh is within /temp/apache-cassandra-3.11.0/bin directory

IFS=$'\n' read -d '' -r -a lines < nodeList.txt

# Bulk load data
cd /temp/apache-cassandra-3.11.0/bin
./cqlsh ${lines[0]} -f ~/Team10/schema.cql

echo -ne "Loading WAREHOUSE, DISTRICT, ORDER-LINE, STOCK-ITEM, PAYMENT_BY_CUSTOMER, ITEM_BY_WAREHOUSE_DISTRICT and ORDER_BY_DESC, ORDER_BY_ASC data\n"

cd ~/Team10/loadData
chmod +x *.py

# Load Warehouse
./LoadWarehouseData.py ${lines[0]}

# Load District
./LoadDistrictData.py ${lines[0]}

# Load StockItem
cqlsh ${lines[0]} -e "copy team10.stockitem (S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, S_DATA) from '~/Team10/data-files/stock.csv';"

# Update Stockitem
./LoadStockItemData.py ${lines[0]}

# Load Payment_by_Customer
./LoadPaymentByCustomerData.py ${lines[0]}

# Load OrderByDesc, OrderByAsc
cp ~/Team10/data-files/order.csv ~/Team10/data-files/order_copy.csv

sed -i -e 's/,null,/,0,/g' -e 's/^null,/0,/' -e 's/0,null,/0,0,/g' -e 's/,null$/,0/' ~/Team10/data-files/order_copy.csv

cqlsh ${lines[0]} -e "copy team10.Order_By_Desc (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) from '~/Team10/data-files/order_copy.csv';";

./LoadOrderData.py ${lines[0]}

# create a dummy csv
cp ~/Team10/data-files/warehouse.csv ~/Team10/data-files/order-line02.csv

cqlsh ${lines[0]} -e "copy team10.Order_By_Desc TO '/home/stuproj/cs4224j/Team10/data-files/order-line02.csv'";

cqlsh ${lines[0]} -e "copy team10.Order_By_Asc FROM '/home/stuproj/cs4224j/Team10/data-files/order-line02.csv';"


# Load OrderLine
cqlsh ${lines[0]} -e "copy team10.orderline (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) from '~/Team10/data-files/order-line.csv' WITH NULL = 'null';";

./LoadOrderLineData.py ${lines[0]}


# Load Item_by_Warehouse_District
cqlsh ${lines[0]} -e "copy team10.district (d_w_id, d_id) TO '/home/stuproj/cs4224j/Team10/data-files/order-line02.csv'";

cqlsh ${lines[0]} -e "copy team10.item_by_warehouse_district (w_id, d_id) FROM '/home/stuproj/cs4224j/Team10/data-files/order-line02.csv';"

./LoadItemByWarehouseDistrictData.py ${lines[0]}

