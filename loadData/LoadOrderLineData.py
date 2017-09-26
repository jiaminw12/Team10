#!/usr/bin/env python
import csv
from decimal import *
from datetime import datetime, date, time
import time
from cassandra.cluster import Cluster
import sys


def getdata(filename):
    with open(filename, "rb") as csvfile:
        datareader = csv.reader(csvfile)
        for row in datareader:
            yield row

#  Start of program
cluster = Cluster();
session = cluster.connect('team10')


select_statement = session.prepare("SELECT count(ol_number) AS count FROM orderline WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?");

print("Updating OrderLine data ... ")
for row in getdata('../data-files/order.csv'):

    o_c_id = int(row[3])
    o_all_local = Decimal(row[6])
    if row[4] != 'null':
        o_carrier_id = int(row[4])
    else:
        o_carrier_id = 0
    o_entry_d = datetime.strptime(row[7], '%Y-%m-%d %H:%M:%S.%f')
    o_ol_cnt = Decimal(row[5])
    o_w_id = int(row[0])
    o_d_id = int(row[1])
    o_id = int(row[2])

    result_count = session.execute(select_statement, [o_w_id, o_d_id, o_id])
    for row in result_count:
        ol_number = int(row[0])

    str_list = []
    for i in range (1, ol_number+1):
        str_list.append(`i`)
        str = ','.join(str_list)

    update_statement = session.prepare("UPDATE orderline SET o_c_id = ?, o_all_local = ?, o_carrier_id = ?, o_entry_d = ?, o_ol_cnt = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ? AND ol_number IN (" +  str + ")");

    session.execute(update_statement, [o_c_id, o_all_local, o_carrier_id, o_entry_d, o_ol_cnt, o_w_id, o_d_id, o_id])

print("Updating Done ... ")


cluster.shutdown();








