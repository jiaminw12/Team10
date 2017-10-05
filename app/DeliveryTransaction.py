#!/usr/bin/env python
from cassandra.cluster import Cluster
from datetime import datetime
import decimal

class DeliveryTransaction(object):
	# TODO(CF) There is no proper value to select for null o_carrier_id
	def __init__(self, session, w_id, carrier_id):
		self.session = session
		self.w_id = int(w_id)
		self.carrier_id = int(carrier_id)
		self.initPreparedStmts()

	def initPreparedStmts(self):
		self.min_order_number_query = self.session.prepare(
				"SELECT o_id, o_c_id FROM team10.orderline "
				"WHERE o_w_id = ? AND o_d_id = ? AND o_carrier_id = 0 "
				"LIMIT 1 ALLOW FILTERING")
		self.select_all_ols_by_order = self.session.prepare(
				"SELECT ol_number, ol_amount FROM team10.orderline "
				"WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?")
		self.update_delivery_by_customer_query = self.session.prepare(
				"UPDATE team10.delivery_by_customer SET o_carrier_id = ?, ol_delivery_d = ?"
				"WHERE o_w_id = ? AND o_d_id = ? AND o_id = ? AND ol_number = ?")
		self.update_order_line_query = self.session.prepare(
				"UPDATE team10.orderline SET o_carrier_id = ?"
				"WHERE o_w_id = ? AND o_d_id = ? AND o_id = ? AND ol_number = ?")
		self.select_payment_by_customer_query = self.session.prepare(
				"SELECT c_balance, c_delivery_cnt FROM team10.payment_by_customer "
				"WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")
		self.update_payment_by_customer_query = self.session.prepare(
				"UPDATE team10.payment_by_customer SET c_balance = ?, c_delivery_cnt= ? "
				"WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")

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
				self.update_delivery_by_customer(district_no, ol_in_order[0])
				self.update_order_line(district_no, ol_in_order[0])
			self.ol_amount = ol_amount
			self.update_payment_by_customer(district_no)

	def update_delivery_by_customer(self, district_no, ol_number):
		self.session.execute(self.update_delivery_by_customer_query, (self.carrier_id, datetime.now(), self.w_id, district_no, self.X, ol_number))

	def update_order_line(self, district_no, ol_number):
		self.session.execute(self.update_order_line_query, (self.carrier_id, self.w_id, district_no, self.X, ol_number))

	def update_payment_by_customer(self, district_no):
		customer_attr = self.session.execute(self.select_payment_by_customer_query, (self.w_id, district_no, self.C))
		self.c_balance = customer_attr[0][0] + decimal.Decimal(self.ol_amount)
		self.c_delivery_cnt = customer_attr[0][1] + 1
		self.session.execute(self.update_payment_by_customer_query, (self.c_balance, self.c_delivery_cnt, self.w_id, district_no, self.C))


