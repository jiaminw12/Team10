#!/usr/bin/env python

from cassandra.cluster import Cluster

#cluster = Cluster(['192.168.0.1', '192.168.0.2'])
cluster = Cluster();

# create a class to map to the "address" UDT
class Address(object):
    def __init__(self, street_1, street_2, city, state, zip):
        self.street_1 = street_1
        self.street_2 = street_2
        self.city = city
        self.state = state
        self.zip = zip


class Connect(object):
    
    global session;
    global cluster;
    
    def __init__(self, keyspace):
        self.keyspace = keyspace
        self.session = cluster.connect(keyspace)
        cluster.register_user_type(keyspace, 'address', Address)

    def getSession(self):
        return self.session;

    def close(self):
        self.session.shutdown();
        cluster.shutdown();






