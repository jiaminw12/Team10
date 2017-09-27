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
popularItemXact = 'I'
topBalanceXact = 'T'

# Connect Keyspace
Connect('team10')
session = Connect.getSession()

#for row in getdata('../xact-files/%2$s.txt'):
 with open('../xact-files/%2$s.txt', 'r+') as myFile:
    lines = myFile.readlines()
    
    for i in range(0, len(lines)):
        line = lines[i]
        str = line.split(',')

        if str[0] == newOrderXact:
            w_id = int(str[1]);
            d_id = int(str[2]);
            c_id = int(str[3]);
            needToRead = int(str[4]) #Num of lines needed to read
            
            newOrderList = []
            for j in range(0, needToRead):
                newOrderList.append(lines[i+j+1]);
                
            newOrder(w_id, d_id, c_id, newOrderList);
        elif str[0] == paymentXact:
            c_w_id = str[1];
            c_d_id = str[2];
            c_id = str[3];
            amount = str[4];
            
            payment(c_w_id, c_d_id, c_id, amount);
            
        elif str[0] == deliveryXact:
             w_id = str[1];
             carrier_id = str[2];
             delivery(w_id, carrier_id);
                
        elif str[0] == orderStatusXact:
            c_w_id = str[1];
            c_d_id = str[2];
            c_id = str[3];
            
            orderStatus(c_w_id, c_d_id, c_id);
            
        elif str[0] == stockLevelXact:
            w_id = str[1];
            d_id = str[2];
            threshold = str[3];
            numLastOrders = str[4];
            
            stockLevel(w_id, d_id, threshold, numLastOrders);
            
        elif str[0] == popularItemXact:
            w_id = str[1];
            d_id = str[2];
            numLastOrders = str[3];
            popularItem(w_id, d_id, numLastOrders);
            
        elif str[0] == topBalanceXact:
            topBalance();

# New Order Transaction
def newOrder(session, w_id, d_id, c_id, newOrderList):
	
	num_items = len(newOrderList);
	i_id_list = [];
	supplier_w_id_list = [];
	quantity_list = [];
	
	for strOrder in newOrderList:
		orderArray = strOrder.split(',');
		i_id_list.append(int(orderArray[0]));
		supplier_w_id_list.append(int(orderArray[1]));
		quantity_list.append(int(orderArray[2]));
	
	NewOrderTransaction(w_id, d_id, c_id, num_items, i_id_list, supplier_w_id_list, quantity_list);
	NewOrderTransaction.process();


def payment(session, c_w_id, c_d_id, c_id, amount):
    c_w_id = int(strArray[1])
    c_d_id = int(strArray[2])
    c_id = int(strArray[3])
    payment = Decimal(strArray[4])

    PaymentTransaction(c_w_id, c_d_id, c_id, payment)
    PaymentTransaction.process()


def delivery(session, w_id, carrier_id):
    w_id = int(strArray[1])
    carrier_id = int(strArray[2])

    DeliveryTransaction(w_id, carrier_id)
    DeliveryTransaction.process()


def orderStatus(session, c_w_id, c_d_id, c_id):
    c_w_id = int(strArray[1])
    c_d_id = int(strArray[2])
    c_id = int(strArray[3])

    OrderStatusTransaction(c_w_id, c_d_id, c_id)
    OrderStatusTransaction.process()


def stockLevel(session, w_id, d_id, threshold, numLastOrders):
    w_id = int(strArray[1])
    d_id = int(strArray[2])
    stockThreshold = int(strArray[3])
    numOfLastOrder = int(strArray[4])

    StockLevelTransaction(w_id, d_id, stockThreshold, numOfLastOrder)
    StockLevelTransaction.process()


def popularItem(session, w_id, d_id, numLastOrders):
    w_id = int(strArray[1])
    d_id = int(strArray[2])
    numOfLastOrder = int(strArray[3])

    PopularItemTransaction(w_id, d_id, numOfLastOrder)
    PopularItemTransaction.process()


def topBalance(session):
    TopBalanceTransaction()
    TopBalanceTransaction.process()














