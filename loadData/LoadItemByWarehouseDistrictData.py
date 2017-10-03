#!/usr/bin/env python
import csv
from decimal import *
from cassandra.cluster import Cluster

def getdata(filename):
    with open(filename, "rb") as csvfile:
        datareader = csv.reader(csvfile)
        for row in datareader:
            yield row

#  Start of program
#cluster = Cluster(['192.168.0.1', '192.168.0.2'])
cluster = Cluster();
session = cluster.connect('team02')

str_list = []
for i in range (1, 100001):
    str_list.append(`i`)
    str = ','.join(str_list)


# stock
update_stock_statement = session.prepare("UPDATE item_by_warehouse_district SET s_quantity = ? WHERE w_id = ? AND i_id = ? AND d_id IN (1,2,3,4,5,6,7,8,9,10)");

print("Updating Stock data ... ")
for row in getdata('../data-files/stock.csv'):
    
    s_w_id = int(row[0])
    s_i_id = int(row[1])
    s_quantity = Decimal(row[2])
    
    session.execute(update_stock_statement, [s_quantity, s_w_id, s_i_id])
print("Inserting Done ... ")


# item
update_item_statement = session.prepare("UPDATE item_by_warehouse_district SET i_name = ?, i_price = ? WHERE w_id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16) AND d_id IN (1,2,3,4,5,6,7,8,9,10) AND i_id = ?");

print("Updating Item data ... ")
for row in getdata('../data-files/item.csv'):
    
    i_id = int(row[0])
    i_price = Decimal(row[2])
    
    session.execute(update_item_statement, [row[1], i_price, i_id])
print("Inserting Done ... ")


# warehouse
update_warehouse_statement = session.prepare("UPDATE item_by_warehouse_district SET w_tax = ? WHERE w_id = ? AND d_id = ? AND i_id IN (" + str + ")");

print("Updating Warehouse data ... ")
for row in getdata('../data-files/warehouse.csv'):
    
    w_id = int(row[0])
    w_tax = Decimal(row[7])
    
    for d_id in range(1, 11):
        session.execute(update_warehouse_statement, [w_tax, w_id, d_id])
print("Inserting Done ... ")


# district
update_district_statement = session.prepare("UPDATE item_by_warehouse_district SET d_next_o_id = ?, d_tax = ? WHERE w_id = ? AND d_id = ? AND i_id IN (" + str + ")");

print("Updating District data ... ")
for row in getdata('../data-files/district.csv'):
    
    d_w_id = int(row[0])
    d_id = int(row[1])
    d_tax = Decimal(row[8])
    d_next_o_id = int(row[10])
    
    session.execute(update_district_statement, [d_next_o_id, d_tax, d_w_id, d_id])
print("Updating Done ... ")

cluster.shutdown();
