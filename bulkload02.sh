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

echo -ne "Loading WAREHOUSE, DISTRICT, ORDER-LINE, STOCK-ITEM, PAYMENT_BY_CUSTOMER ITEM_BY_WAREHOUSE_DISTRICT and DELIVERY_BY_CUSTOMER data\n"

cqlsh ${lines[0]} -e "copy team10.district from '/home/stuproj/cs4224j/Team10/data-files-backup/district.csv';"

cqlsh ${lines[0]} -e "copy team10.warehouse from '/home/stuproj/cs4224j/Team10/data-files-backup/warehouse.csv'"

cqlsh ${lines[0]} -e "copy team10.item_by_warehouse_district FROM '/home/stuproj/cs4224j/Team10/data-files-backup/item_by_warehouse_district.csv';"

cqlsh ${lines[0]} -e "copy team10.order_by_desc from '/home/stuproj/cs4224j/Team10/data-files-backup/order_by_desc.csv'"

cqlsh ${lines[0]} -e "copy team10.order_by_asc from '/home/stuproj/cs4224j/Team10/data-files-backup/order_by_asc.csv'"

cqlsh ${lines[0]} -e "copy team10.orderline from '/home/stuproj/cs4224j/Team10/data-files-backup/orderline.csv'"

cqlsh ${lines[0]} -e "copy team10.stockitem from '/home/stuproj/cs4224j/Team10/data-files-backup/stockitem.csv'"

cqlsh ${lines[0]} -e "copy team10.payment_by_customer from '/home/stuproj/cs4224j/Team10/data-files-backup/payment_by_customer.csv'"





