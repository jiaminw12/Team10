#!/usr/bin/env python

from cassandra.cluster import Cluster

class StockLevelTransaction():
    
    def __init__(self, w_id, d_id, stockThreshold, numOfLastOrder):
        self.session = session
        self.w_id = w_id
        self.d_id = d_id
        self.stockThreshold = stockThreshold
        self.numOfLastOrder = numOfLastOrder
    
    #def process(self):
    # 





