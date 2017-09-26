#!/usr/bin/env python

from cassandra.cluster import Cluster

class NewOrderTransaction():

    def __init__(self, w_id, d_id, c_id, num_items, item_num, supplier_warehouse, quantity):
        self.w_id = w_id
        self.d_id = d_id
        self.c_id = c_id
        self.num_items = num_items
        self.item_num = item_num
        self.supplier_warehouse = supplier_warehouse
        self.quantity = quantity

    def process(self):
    # Need to update/insert record orderline & delivery_by_customer when is related to order or orderline







