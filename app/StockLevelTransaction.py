#!/usr/bin/env python

from cassandra.cluster import Cluster
from time import gmtime, strftime

class StockLevelTransaction(object):
    
    def __init__(self, w_id, d_id, stockThreshold, numOfLastOrder):
        self.w_id = w_id
        self.d_id = d_id
        self.stockThreshold = stockThreshold
        self.numOfLastOrder = numOfLastOrder
    
    def process(self):
        # prepare statement
        # get the next_o_id N
        select_d_next_o_id = session.prepare("SELECT D_NEXT_O_ID FROM ITEM_BY_WAREHOUSE_DISTRICT WHERE W_ID = ? AND D_ID = ?")
        # get set of items from the last L orders
        select_last_l_order = session.prepare("SELECT OL_I_ID FROM ORDER_LINE WHERE O_W_ID = ? AND O_D_ID = ? AND O_O_ID >= (? - ?) AND O_ID < ?")

        # get total number of items in S where its stock quantity < T
        select_quantity = session.prepare("SELECT count(distinct I_id)as countno FROM ITEM_BY_WAREHOUSE_DISTRICT WHERE I_ID = ? AND S_QUANTITY < ?")

        # execute
        next_oid = session execute(select_d_next_o_id,(self.w_id, self.d_id))
        next_o_id = next_oid[0]
        last_l_orders = session.execute(select_last_l_order,(self.w_id,self.d_id,self.next_o_id,self.numOfLastOrder, self.next_o_id))
        length = len(last_l_orders)
            for x in range (0, length):
                row = last_l_orders[x]
                execute_count = session.execute(select_quantity,(row.OL_I_ID, self.stockThreshold))
                    if execute_count>0:
                        print "Item ID: %d \nNumber: %d\n"(row.OL_I_ID, execute_count.countno)







