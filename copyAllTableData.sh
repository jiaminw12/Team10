#!/bin/bash

IFS=$'\n' read -d '' -r -a lines < nodeList.txt

declare -r DATA_FILES_FOLDER="data-files-backup"

if [ -d $DATA_FILES_FOLDER ];
then
	echo -ne "yes \n"
else
	mkdir data-files-backup
	cd data-files-backup

	cqlsh ${lines[0]} -e "copy team10.warehouse TO 'warehouse.csv';"
	cqlsh ${lines[0]} -e "copy team10.district TO 'district.csv';"
	cqlsh ${lines[0]} -e "copy team10.delivery_by_customer TO 'delivery_by_customer.csv';"
	cqlsh ${lines[0]} -e "copy team10.orderline TO 'orderline.csv';"
	cqlsh ${lines[0]} -e "copy team10.stockitem TO 'stockitem.csv';"
	cqlsh ${lines[0]} -e "copy team10.payment_by_customer TO 'payment_by_customer.csv';"
	cqlsh ${lines[0]} -e "copy team10.item_by_warehouse_district TO 'item_by_warehouse_district.csv';"
fi

