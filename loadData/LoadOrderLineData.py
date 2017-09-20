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
cluster = Cluster();
session = cluster.connect('team10')

# Remove o_c_id from primary key, as cannot update PRIMARY KEY
insert_statement = session.prepare("INSERT INTO orderline (o_w_id, o_d_id, o_id, o_all_local, o_c_id, o_carrier_id, o_entry_d, o_ol_cnt, ol_amount, ol_dist_info, ol_i_id, ol_number, ol_quantity, ol_supply_w_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

update_statement = session.prepare("UPDATE orderline SET o_c_id = ?, o_all_local = ?, o_carrier_id = ?, o_entry_d = ?, o_ol_cnt = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?");

# Load Stock data first then update the row
for row in getdata('../data-files/order-line.csv'):
    
    o_w_id = int(row[0])
    o_d_id = int(row[1])
    o_id = int(row[2])
    ol_amount = Decimal(row[6])
    ol_i_id = int(row[4])
    ol_number = int(row[3])
    ol_quantity = Decimal(row[8])
    ol_supply_w_id = int(row[7])
    
    # Aggregation query used without partition key
    # take quite long to finish this process
    session.execute(insert_statement, [o_w_id, o_d_id, o_id, 0, 0, 0, 0, 0, ol_amount, row[9], ol_i_id, ol_number, ol_quantity, ol_supply_w_id])

for row in getdata('../data-files/order.csv'):
    
    o_c_id = int(row[3])
    o_all_local = Decimal(row[6])
    o_carrier_id = int(row[4])
    o_ol_cnt = Decimal(row[5])
    
    session.execute(update_statement, [o_c_id, o_all_local, o_carrier_id, row[7], o_ol_cnt, row[0], row[1], row[2]])


cluster.shutdown();








