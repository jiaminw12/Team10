#!/usr/bin/env python

from cassandra.cluster import Cluster

class NewOrderTransaction():

    def __init__(self, session, w_id, d_id, c_id, num_items, i_id_list, supplier_w_id_list, quantity_list):
        self.session = session
        self.w_id = w_id
        self.d_id = d_id
        self.c_id = c_id
        self.num_items = num_items
        self.i_id_list = i_id_list
        self.supplier_w_id_list = supplier_w_id_list
        self.quantity_list = quantity_list

    def process(self):
    # Need to update/insert record orderline & delivery_by_customer when is related to order or orderline







