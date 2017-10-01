#!/usr/bin/env python

from cassandra.cluster import Cluster
from decimal import *

class PopularItemTransaction(object):
	
	global popular_item

	def __init__(self, session, w_id, d_id, numOfLastOrder):
		self.session = session
		self.w_id = w_id
		self.d_id = d_id
		self.numOfLastOrder = numOfLastOrder

	def process(self):
		
		popular_item = dict()
		
		print "w_id: %s, d_id: %s"%(self.w_id, self.d_id)
		print "L: %s"%(self.numOfLastOrder)

		# 1. Find N - the next available order number
		select_d_next_o_id = self.session.prepare("SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?");
		result_d_next_o_id = self.session.execute(select_d_next_o_id, [int(self.w_id), int(self.d_id)])
		nextAvailableOrderNum = int(result_d_next_o_id[0].d_next_o_id)

		# 2. Find S - the set of last L orders and order lines for district
		startOrderId = nextAvailableOrderNum - int(self.numOfLastOrder)

		select_last_order_id = self.session.prepare("SELECT o_id FROM delivery_by_customer WHERE o_w_id = ? AND o_d_id = ? AND o_id >= ? AND o_id < ?")

		result_last_order_id = self.session.execute(select_last_order_id, [int(self.w_id), int(self.d_id), startOrderId, nextAvailableOrderNum])
		
		# Remove duplicated order number
		numList = []
		for rowOId in result_last_order_id:
			numList.append(rowOId[0])

		numList = list(set(numList))
		
		for row in numList:

			o_id = int(row)
			
			select_last_order = self.session.prepare("SELECT o_c_id, o_entry_d FROM delivery_by_customer WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?")
		
			result_last_order = self.session.execute(select_last_order, [int(self.w_id), int(self.d_id), o_id])
			
			o_c_id = int(result_last_order[0].o_c_id)
			o_entry_d = result_last_order[0].o_entry_d

			print "o_id: %d, o_entry_d: %s"%(o_id, o_entry_d)
			
			select_customer_name = self.session.prepare("SELECT c_first, c_middle, c_last FROM payment_by_customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")
			
			result_customer_name = self.session.execute(select_customer_name, [int(self.w_id), int(self.d_id), o_c_id])
			
			print "C_FIRST: %s,  C_MIDDLE: %s,  C_LAST: %s"%(result_customer_name[0].c_first, result_customer_name[0].c_middle, result_customer_name[0].c_last)
			
			# 3. Find max quantity
			select_max_quantity = self.session.prepare("SELECT max(ol_quantity) as max_quantity FROM orderline WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?");
			result_max_quantity= self.session.execute(select_max_quantity, [int(self.w_id), int(self.d_id), o_id])
			max_num = int(result_max_quantity[0].max_quantity)
			
			# 4. Get the set with max quantity
			select_max_quantity_item  = self.session.prepare("SELECT ol_i_id, ol_quantity FROM orderline WHERE o_w_id = ? AND o_d_id = ? AND o_id = ? AND ol_quantity = ? ALLOW FILTERING");
			result_max_quantity_item= self.session.execute(select_max_quantity_item, [int(self.w_id), int(self.d_id), o_id, max_num])
			
			for rowPopular in result_max_quantity_item:
				
				# 5. Find item name
				select_item_name = self.session.prepare("SELECT i_name FROM item_by_warehouse_district WHERE w_id = ? AND d_id = ? AND i_id = ?");
				result_item_name= self.session.execute(select_item_name, [int(self.w_id), int(self.d_id), rowPopular.ol_i_id])
				
				#print "I_ID: %d"%(rowPopular.ol_i_id)
				print "I_NAME: %s"%(result_item_name[0].i_name)
				print "OL_QUANTITY: %f"%(rowPopular.ol_quantity)
				print "\n"
	
				if result_item_name[0].i_name in popular_item:
					popular_item[result_item_name[0].i_name] += 1
				else:
					popular_item[result_item_name[0].i_name] = 1


		# 6. Find the percentage of examined of orders that contain each popular item
		#print (popular_item)

		for key, value in popular_item.iteritems():
			print "I_NAME: %s"%(key)
			print "Percentage of orders in S: %f"%(Decimal(int(value)/int(self.numOfLastOrder)*100))
			print "\n"







