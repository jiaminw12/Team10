#!/usr/bin/env python

from cassandra.cluster import Cluster

class OrderStatusTransaction(object):
    
    def __init__(self, c_w_id, c_d_id, c_id):
        self.c_w_id = c_w_id
        self.c_d_id = c_d_id
        self.c_id = c_id
    
    def process(self):





