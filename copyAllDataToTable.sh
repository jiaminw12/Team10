#!/bin/bash

cqlsh -e "copy team10.warehouse FROM 'warehouse.csv';"
cqlsh -e "copy team10.district FROM 'district.csv';"
cqlsh -e "copy team10.delivery_by_customer FROM 'delivery_by_customer.csv';"
cqlsh -e "copy team10.orderline FROM 'orderline.csv';"
cqlsh -e "copy team10.stockitem FROM 'stockitem.csv';"
cqlsh -e "copy team10.payment_by_customer FROM 'payment_by_customer.csv';"
cqlsh -e "copy team10.item_by_warehouse_district FROM 'item_by_warehouse_district.csv';"

