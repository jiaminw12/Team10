#!/usr/bin/env python
import csv
from decimal import *
from cassandra.cluster import Cluster

# create a class to map to the "address" UDT
class Address(object):
    def __init__(self, street_1, street_2, city, state, zip):
        self.street_1 = street_1
        self.street_2 = street_2
        self.city = city
        self.state = state
        self.zip = zip

def getdata(filename):
    with open(filename, "rb") as csvfile:
        datareader = csv.reader(csvfile)
        for row in datareader:
            yield row

#  Start of program
#cluster = Cluster(['192.168.0.1', '192.168.0.2'])
cluster = Cluster();
session = cluster.connect('team10')

cluster.register_user_type('team10', 'address', Address)

insert_statement = session.prepare("INSERT INTO district (d_w_id, d_id, d_name, d_address, d_tax, d_ytd, d_next_o_id) VALUES (?, ?, ?, ?, ?, ?, ?)")

print("Inserting District data ... ")
for row in getdata('../data-files/district.csv'):
    
    d_w_id = int(row[0])
    d_id = int(row[1])
    d_tax = Decimal(row[8])
    d_ytd = Decimal(row[9])
    d_next_o_id = int(row[10])
    
    session.execute(insert_statement, [d_w_id, d_id, row[2], Address(row[3], row[4], row[5], row[6], row[7]), d_tax, d_ytd, d_next_o_id])

print("Inserting Done ... ")
cluster.shutdown();








