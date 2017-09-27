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
        
        self.initPreparedStmts()

    def initPreparedStmts(self):
        
        self.update_district_stmt = self.session.prepare("UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = ? AND d_id = ?");
        self.update_item_by_warehouse_district_stmt = self.session.prepare("UPDATE item_by_warehouse_district SET d_next_o_id = d_next_o_id + 1 WHERE w_id = ? AND d_id = ? AND i_id = ?");
    
    def process(self):
        
        
    
    # Increment d_next_o_id by 1 in table DISTRICT and ITEM_BY_WAREHOUSE_DISTRICT
    def incrementNextOrderId(self):
        self.session.execute()
        
    # Need to update/insert record orderline & delivery_by_customer when is related to order or orderline







