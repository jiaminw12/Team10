#!/usr/bin/env python

from cassandra.cluster import Cluster

class OrderStatusTransaction(object):
    
    def __init__(self, session, c_w_id, c_d_id, c_id):
        self.session = session
        self.c_w_id = c_w_id
        self.c_d_id = c_d_id
        self.c_id = c_id
    def process(self):
        # get the customer's last order
        self.select_customer_las_order = session.prepare("SELECT c_first, c_middle, c_last, c_balance from PSYMENT_BY_CUSTOMER WHERE w_id = c_w_id and d_id = c_d_id and c_id = c_id")
        self.select_order = session.prepare("SELECT o_id,o_entry_d, o_carrier_id, ol_i_id, ol_supply_w_id, ol_quantity,ol_amount,ol_delivery_d from delivery_by_customer where w_id = c_w_id AND d_id = c_d_id AND c_id = c_id limit 1")











