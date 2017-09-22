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

update_statement = session.prepare("UPDATE stockitem SET i_data = ?, i_im_id = ?, i_price = ? WHERE S_W_ID IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16) AND S_I_ID = ?");

print("Updating StockItem data ... ")
for row in getdata('../data-files/item.csv'):
    
    s_i_id = int(row[0])
    i_im_id = int(row[3])
    i_price = Decimal(row[2])
    
    session.execute(update_statement, [row[4], i_im_id, i_price, s_i_id])

print("Updating Done ... ")
cluster.shutdown();








