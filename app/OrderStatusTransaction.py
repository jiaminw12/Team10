#!/usr/bin/env python

from cassandra.cluster import Cluster
from time import gmtime, strftime
class OrderStatusTransaction(object):
    
    def __init__(self, c_w_id, c_d_id, c_id):
        self.c_w_id = c_w_id
        self.c_d_id = c_d_id
        self.c_id = c_id
    self.process()
    
    def process(self):
        # get the customer's last order
        select_customer_las_order = session.prepare("SELECT c_first, c_middle, c_last, c_balance from PSYMENT_BY_CUSTOMER WHERE w_id = ? and d_id = ? and c_id = ?")
        
        # get the last order id
        select_last_order = session.prepare("select o_id from delivery_by_customer where w_id = ? AND d_id = ? AND c_id = ? Limit 1")
        orderid = session.execute(select_last_order,(self.c_w_id, self.c_d_id, self_c_id))
        lastorder = orderid[0]
        
        select_order = session.prepare("SELECT o_id,o_entry_d, o_carrier_id, ol_i_id, ol_supply_w_id, ol_quantity,ol_amount,ol_delivery_d from delivery_by_customer where w_id = ? AND d_id = ? AND c_id = ? AND o_id = ?")
        # execute
        customerdetails = session.execute(self.select_customer_las_order,(self.w_id,self.c_d_id,self.c_id));
        # print output 1. customer name and balance 2. its order
        customer = customerdetails[0]
        print "Customer name: %s %s %s, Balance amount: %f\n"(customer.c_first, customer.c_middle, customer.c_last, customer.c_balance)

        # print order with items
        otderdetails = session.execute(self.select_order,(self.c_w_id,self.c_d_id,self.c_id, lastorder.o_id))
        length = len(otderdetails)
        temp = otderdetails[0];
        print "order ID: %d\t Entry Date: %s\t Carrier ID: %d\t\n"(temp.o_id, temp.o_entry_d,temp.o_carrier_id)
            for x in range(0,length):
                row = otderdetails[x]
                print "Item ID: %d\t Supply Warehouse ID: %d\t Quantity: %d\t Amount: %f\t Delivery Date: %s\n"(row.ol_i_id, row.ol_supply_w_id, row.ol_quantity, row.ol_amount,row.ol_delivery_d)










