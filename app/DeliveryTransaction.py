#!/usr/bin/env python
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from datetime import datetime
import decimal

class DeliveryTransaction(object):
	def __init__(self, session, consistencyLevel, w_id, carrier_id):
		self.session = session
		self.consistencyLevel = consistencyLevel
		self.w_id = int(w_id)
		self.carrier_id = int(carrier_id)
		self.initPreparedStmts()

	def initPreparedStmts(self):
		self.min_order_number_query = self.session.prepare(
				"SELECT o_id, o_c_id FROM order_by_asc "
				"WHERE o_w_id = ? AND o_d_id = ? AND o_carrier_id = 0 "
				"LIMIT 1 ALLOW FILTERING")
		self.select_all_ols_by_order = self.session.prepare(
				"SELECT ol_number, ol_amount FROM orderline "
				"WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?")
		self.update_order_by_desc_query = self.session.prepare(
				"UPDATE order_by_desc SET o_carrier_id = ?"
				"WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?")
		self.update_order_by_asc_query = self.session.prepare(
                                "UPDATE order_by_asc SET o_carrier_id = ?"
                                "WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?")
		self.update_order_line_query = self.session.prepare(
				"UPDATE orderline SET ol_delivery_d = ?"
				"WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_number = ?")
		self.select_payment_by_customer_query = self.session.prepare(
				"SELECT c_balance, c_delivery_cnt FROM payment_by_customer "
				"WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")
		self.update_payment_by_customer_query = self.session.prepare(
				"UPDATE payment_by_customer SET c_balance = ?, c_delivery_cnt= ? "
				"WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")
	
		if self.consistencyLevel == '1' :
			self.min_order_number_query.consistency_level = ConsistencyLevel.ONE
			self.select_all_ols_by_order.consistency_level = ConsistencyLevel.ONE
			self.update_order_by_desc_query.consistency_level = ConsistencyLevel.ONE
			self.update_order_by_asc_query.consistency_level = ConsistencyLevel.ONE
			self.update_order_line_query.consistency_level = ConsistencyLevel.ONE
			self.select_payment_by_customer_query.consistency_level = ConsistencyLevel.ONE
			self.update_payment_by_customer_query.consistency_level = ConsistencyLevel.ONE
		else:
			self.min_order_number_query.consistency_level = ConsistencyLevel.QUORUM
			self.select_all_ols_by_order.consistency_level = ConsistencyLevel.QUORUM
			self.update_order_by_desc_query.consistency_level = ConsistencyLevel.QUORUM
                        self.update_order_by_asc_query.consistency_level = ConsistencyLevel.QUORUM
			self.update_order_line_query.consistency_level = ConsistencyLevel.QUORUM
			self.select_payment_by_customer_query.consistency_level = ConsistencyLevel.QUORUM
			self.update_payment_by_customer_query.consistency_level = ConsistencyLevel.QUORUM

	def process(self):
		for district_no in range(1, 11):
			self.get_next_order_and_customer(district_no)

	def get_next_order_and_customer(self, district_no):
		min_order = self.session.execute(self.min_order_number_query, (self.w_id, district_no))
		
		if min_order:
			self.X = min_order[0][0]
			self.C = min_order[0][1]
			ol_amount = 0.00
			for ol_in_order in self.session.execute(self.select_all_ols_by_order, (self.w_id, district_no, self.X)):
				ol_amount = ol_amount + float(ol_in_order[1])
				self.update_order_by_desc_asc(district_no)
				self.update_order_line(district_no, ol_in_order[0])
			self.ol_amount = ol_amount
			self.update_payment_by_customer(district_no)

	def update_order_by_desc_asc(self, district_no):
		self.session.execute(self.update_order_by_desc_query, (self.carrier_id, self.w_id, district_no, self.X))
		self.session.execute(self.update_order_by_asc_query, (self.carrier_id, self.w_id, district_no, self.X))

	def update_order_line(self, district_no, ol_number):
		self.session.execute(self.update_order_line_query, (datetime.now(), self.w_id, district_no, self.X, ol_number))

	def update_payment_by_customer(self, district_no):
		customer_attr = self.session.execute(self.select_payment_by_customer_query, (self.w_id, district_no, self.C))
		self.c_balance = customer_attr[0][0] + decimal.Decimal(self.ol_amount)
		self.c_delivery_cnt = customer_attr[0][1] + 1
		self.session.execute(self.update_payment_by_customer_query, (self.c_balance, self.c_delivery_cnt, self.w_id, district_no, self.C))


