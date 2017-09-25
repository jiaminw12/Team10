#!/usr/bin/env python

from decimal import *
from cassandra.cluster import Cluster
from ConnectCassandra import Connect
from NewOrderTransaction import NewOrderTransaction
from PaymentTransaction import PaymentTransaction
from DeliveryTransaction import DeliveryTransaction
from OrderStatusTransaction import OrderStatusTransaction
from StockLevelTransaction import StockLevelTransaction
from PopularItemTransaction import PopularItemTransaction
from TopBalanceTransaction import TopBalanceTransaction

# function - read file
def getdata(filename):
    with open(filename) as f:
        content = f.read()
        #content = f.readlines()
        yield content


#function - Output Performance Measurement after experiments
def performanceMeasurement():
    # for each client
        # (a) report num of executed transactions
        # (b) total transaction execution time (in sec)
        # transaction throughput -> b / a
    # min, avg, max transaction throughputs among NC clients


newOrderXact = 'N'
paymentXact = 'P'
deliveryXact = 'D'
orderStatusXact = 'O'
stockLevelXact = 'S'
popularItemXact = 'I
topBalanceXact = 'T'

# Connect Keyspace
Connect['team10']
session = Connect.getSession()

for row in getdata('../xact-files/%2$s.txt'):
    strArray = row.split(',')

    if str[0] == 'N'
        newOrder(strArray);
    elif str[0] == 'P'
        payment(strArray);
    elif str[0] == 'D'
        delivery(strArray);
    elif str[0] == 'O'
        orderStatus(strArray);
    elif str[0] == 'S'
        stockLevel(strArray);
    elif str[0] == 'I'
        payment(strArray);
    elif str[0] == 'T'
        popularItem(strArray);

# New Order Transaction
def newOrder(strArray):
    w_id = int(strArray[1])
    d_id = int(strArray[2])
    c_id = int(strArray[3])
    num_items = int(strArray[4])
    item_num[] = int(strArray[5])
    supplier_warehouse[] = int(strArray[6])
    quantity[] = Decimal(strArray[7])

    NewOrderTransaction(w_id, d_id, c_id, num_items, item_num, supplier_warehouse, quantity)
    NewOrderTransaction.process()


def payment(strArray):
    c_w_id = int(strArray[1])
    c_d_id = int(strArray[2])
    c_id = int(strArray[3])
    payment = Decimal(strArray[4])

    PaymentTransaction(c_w_id, c_d_id, c_id, payment)
    PaymentTransaction.process()


def delivery(strArray):
    w_id = int(strArray[1])
    carrier_id = int(strArray[2])

    DeliveryTransaction(w_id, carrier_id)
    DeliveryTransaction.process()


def orderStatus(strArray):
    c_w_id = int(strArray[1])
    c_d_id = int(strArray[2])
    c_id = int(strArray[3])

    OrderStatusTransaction(c_w_id, c_d_id, c_id)
    OrderStatusTransaction.process()


def stockLevel(strArray):
    w_id = int(strArray[1])
    d_id = int(strArray[2])
    stockThreshold = int(strArray[3])
    numOfLastOrder = int(strArray[4])

    StockLevelTransaction(w_id, d_id, stockThreshold, numOfLastOrder)
    StockLevelTransaction.process()


def popularItem(strArray):
    w_id = int(strArray[1])
    d_id = int(strArray[2])
    numOfLastOrder = int(strArray[3])

    PopularItemTransaction(w_id, d_id, numOfLastOrder)
    PopularItemTransaction.process()


def topBalance(strArray):
    TopBalanceTransaction()
    TopBalanceTransaction.process()














