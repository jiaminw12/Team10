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
session = cluster.connect('team10')

# warehouse
update_warehouse_statement = session.prepare("UPDATE item_by_warehouse_district SET w_tax = ? WHERE w_id = ? AND d_id = ?");
print("Updating Warehouse data ... ")
for row in getdata('../data-files/warehouse.csv'):

    w_id = int(row[0])
    w_tax = Decimal(row[7])

    for d_id in range(1, 11):
        session.execute(update_warehouse_statement, [w_tax, w_id, d_id])
print("Inserting Done ... ")


update_district_statement = session.prepare("UPDATE item_by_warehouse_district SET d_tax = ? WHERE w_id = ? AND d_id = ?");
print("Updating District data ... ")
for row in getdata('../data-files/district.csv'):

    d_w_id = int(row[0])
    d_id = int(row[1])
    d_tax = Decimal(row[8])

    session.execute(update_district_statement, [d_tax, d_w_id, d_id])
print("Updating Done ... ")


cluster.shutdown();

