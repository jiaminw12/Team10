#!/usr/bin/env python
import csv
from decimal import *
from datetime import datetime, date, time
import time
from cassandra.cluster import Cluster

def getdata(filename):
    with open(filename, "rb") as csvfile:
        datareader = csv.reader(csvfile)
        for row in datareader:
            yield row

#  Start of program
cluster = Cluster();
session = cluster.connect('team10')

update_date_time_statement = session.prepare("UPDATE delivery_by_customer SET OL_DELIVERY_D = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ? AND ol_number = ? ");

print("Updating Delivery_by_Customer data ... ")
for row in getdata('../data-files/order-line.csv'):
    o_w_id = int(row[0])
    o_d_id = int(row[1])
    o_o_id = int(row[2])
    ol_number = int(row[3])
    if row[5] != 'null':
        ol_delivery_d = datetime.strptime(row[5], '%Y-%m-%d %H:%M:%S.%f')
        session.execute(update_date_time_statement, [ol_delivery_d, o_w_id, o_d_id, o_o_id, ol_number])
                                             
print("Updating Done ... ")
                                             
cluster.shutdown();







