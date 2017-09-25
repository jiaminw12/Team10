#!/usr/bin/env python

from cassandra.cluster import Cluster

class PopularItemTransaction(object):
    
    def __init__(self, w_id, d_id, numOfLastOrder):
        self.w_id = w_id
        self.d_id = d_id
        self.numOfLastOrder = numOfLastOrder
    
    def process(self):





