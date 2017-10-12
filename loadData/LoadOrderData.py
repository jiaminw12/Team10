#!/usr/bin/env python

import sys
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
ipAddr = []
ipAddr.append(sys.argv[1])
cluster = Cluster(ipAddr);
session = cluster.connect('team10')

update_statement = session.prepare("UPDATE OrderByDesc SET o_entry_d = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?");

print("Updating Order data ... ")
for row in getdata('../data-files/order.csv'):

    o_entry_d = datetime.strptime(row[7], '%Y-%m-%d %H:%M:%S.%f')
    o_w_id = int(row[0])
    o_d_id = int(row[1])
    o_id = int(row[2])

    session.execute(update_statement, [o_entry_d, o_w_id, o_d_id, o_id])

print("Updating Done ... ")


cluster.shutdown();








