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

insert_statement = session.prepare("INSERT INTO warehouse (w_id, w_name, w_address, w_tax, w_ytd) VALUES (?, ?, ?, ?, ?)")

print("Inserting Warehouse data ... ")
for row in getdata('../data-files/warehouse.csv'):
    
    w_id = int(row[0])
    w_tax = Decimal(row[7])
    w_ytd = Decimal(row[8])
    
    session.execute(insert_statement, [w_id, row[1], Address(row[2], row[3], row[4], row[5], row[6]), w_tax, w_ytd])

print("Inserting Done ... ")
cluster.shutdown();








