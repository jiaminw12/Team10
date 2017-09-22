#!/usr/bin/env python
import csv
from decimal import *
from datetime import datetime
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

insert_statement = session.prepare("INSERT INTO payment_by_customer (c_w_id, c_d_id, c_id, c_address, c_balance, c_credit, c_credit_lim, c_data, c_delivery_cnt, c_discount, c_first, c_last, c_middle, c_payment_cnt, c_phone, c_since, c_ytd_payment, d_address, w_address) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

for row in getdata('../data-files/customer.csv'):
    
    c_w_id = int(row[0])
    c_d_id = int(row[1])
    c_id = int(row[2])
    c_balance = Decimal(row[16])
    c_credit_lim = Decimal(row[14])
    c_delivery_cnt = int(row[19])
    c_discount = Decimal(row[15])
    c_payment_cnt = int(row[18])
    c_since = datetime.strptime(row[12], '%Y-%m-%d %H:%M:%S.%f')
    c_ytd_payment = Decimal(row[17])
    
    session.execute(insert_statement, [c_w_id, c_d_id, c_id, Address(row[6], row[7], row[8], row[9], row[10]), c_balance, row[13], c_credit_lim, row[20], c_delivery_cnt, c_discount, row[3], row[5], row[4], c_payment_cnt, row[11], c_since, c_ytd_payment, 'null', 'null'])


cluster.shutdown();








