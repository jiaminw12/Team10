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

update_statement = session.prepare("UPDATE orderline SET ol_delivery_d = ? WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_number =?");

print("Updating OrderLine data ... ")
for row in getdata('../data-files/order-line.csv'):

    ol_delivery_d = datetime.strptime(row[5], '%Y-%m-%d %H:%M:%S.%f')
    ol_w_id = int(row[0])
    ol_d_id = int(row[1])
    ol_o_id = int(row[2])
    ol_number = int(row[3])

    session.execute(update_statement, [ol_delivery_d, ol_w_id, ol_d_id, ol_o_id, ol_number])

print("Updating Done ... ")


cluster.shutdown();








