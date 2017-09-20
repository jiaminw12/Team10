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

insert_statement = session.prepare("INSERT INTO stockitem (s_w_id, s_i_id, i_data, i_im_id, i_price, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_order_cnt, s_remote_cnt, s_ytd) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

update_statement = session.prepare("UPDATE stockitem SET i_data = ?, i_im_id = ?, i_price = ? WHERE S_W_ID IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16) AND S_I_ID = ?");

# Load Stock data first then update the row
for row in getdata('../data-files/stock.csv'):
    
    s_w_id = int(row[0])
    s_i_id = int(row[1])
    s_order_cnt = int(row[4])
    s_remote_cnt = int(row[5])
    s_ytd = Decimal(row[3])
    
    # Aggregation query used without partition key
    # take quite long to finish this process
    session.execute(insert_statement, [s_w_id, s_i_id, 'null', 0, 0, row[16], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], s_order_cnt, s_remote_cnt, s_ytd])

for row in getdata('../data-files/item.csv'):
    
    s_i_id = int(row[0])
    i_im_id = int(row[3])
    i_price = Decimal(row[2])
    
    session.execute(update_statement, [row[4], i_im_id, i_price, s_i_id])


cluster.shutdown();








