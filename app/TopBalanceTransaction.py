#!/usr/bin/env python

from cassandra.cluster import Cluster

class TopBalanceTransaction(object):
    
    def __init__(self, session):
        self.session = session
    
    #def process(self):
    # (TOPBALANCE) SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, W_NAME, D_NAME FROM TOP_BALANCE LIMIT 10





