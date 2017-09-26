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

#for row in getdata('../xact-files/%2$s.txt'):
 with open('../xact-files/%2$s.txt', 'r+') as myFile:
    lines = myFile.readlines()
    
    for i in range(0, len(lines)):
        line = lines[i]
        str = line.split(',')

        if str[0] == 'N':
            w_id = str[1];
            d_id = str[2];
            c_id = str[3];
            needToRead = str[4] #Num of lines needed to read
            
            newOrderList = []
            for j in range(0, needToRead):
                newOrderList.append(lines[i+j+1]);
                
            newOrder(w_id, d_id, c_id, newOrderList);
        elif str[0] == 'P':
            c_w_id = str[1];
            c_d_id = str[2];
            c_id = str[3];
            amount = str[4];
            
            payment(c_w_id, c_d_id, c_id, amount);
            
        elif str[0] == 'D':
             w_id = str[1];
             carrier_id = str[2];
             delivery(w_id, carrier_id);
                
        elif str[0] == 'O':
            c_w_id = str[1];
            c_d_id = str[2];
            c_id = str[3];
            
            orderStatus(c_w_id, c_d_id, c_id);
            
        elif str[0] == 'S':
            w_id = str[1];
            d_id = str[2];
            threshold = str[3];
            numLastOrders = str[4];
            
            stockLevel(w_id, d_id, threshold, numLastOrders);
            
        elif str[0] == 'I':
            w_id = str[1];
            d_id = str[2];
            numLastOrders = str[3];
            popularItem(w_id, d_id, numLastOrders);
            
        elif str[0] == 'T':
            topBalance();

# New Order Transaction
def newOrder(w_id, d_id, c_id, newOrderList):
    w_id = int(strArray[1])
    d_id = int(strArray[2])
    c_id = int(strArray[3])
    num_items = int(strArray[4])
    item_num[] = int(strArray[5])
    supplier_warehouse[] = int(strArray[6])
    quantity[] = Decimal(strArray[7])

    NewOrderTransaction(w_id, d_id, c_id, num_items, item_num, supplier_warehouse, quantity)
    NewOrderTransaction.process()


def payment(c_w_id, c_d_id, c_id, amount):
    c_w_id = int(strArray[1])
    c_d_id = int(strArray[2])
    c_id = int(strArray[3])
    payment = Decimal(strArray[4])

    PaymentTransaction(c_w_id, c_d_id, c_id, payment)
    PaymentTransaction.process()


def delivery(w_id, carrier_id):
    w_id = int(strArray[1])
    carrier_id = int(strArray[2])

    DeliveryTransaction(w_id, carrier_id)
    DeliveryTransaction.process()


def orderStatus(c_w_id, c_d_id, c_id):
    c_w_id = int(strArray[1])
    c_d_id = int(strArray[2])
    c_id = int(strArray[3])

    OrderStatusTransaction(c_w_id, c_d_id, c_id)
    OrderStatusTransaction.process()


def stockLevel((w_id, d_id, threshold, numLastOrders)):
    w_id = int(strArray[1])
    d_id = int(strArray[2])
    stockThreshold = int(strArray[3])
    numOfLastOrder = int(strArray[4])

    StockLevelTransaction(w_id, d_id, stockThreshold, numOfLastOrder)
    StockLevelTransaction.process()


def popularItem(w_id, d_id, numLastOrders):
    w_id = int(strArray[1])
    d_id = int(strArray[2])
    numOfLastOrder = int(strArray[3])

    PopularItemTransaction(w_id, d_id, numOfLastOrder)
    PopularItemTransaction.process()


def topBalance():
    TopBalanceTransaction()
    TopBalanceTransaction.process()














