#!/usr/bin/env python

from cassandra.cluster import Cluster

class DeliveryTransaction(object):
    
    def __init__(self, w_id, carrier_id):
        self.w_id = w_id
        self.carrier_id = carrier_id
    
    def process(self):
    # Need to update/insert record orderline & delivery_by_customer when is related to order or orderline




