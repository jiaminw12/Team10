#!/usr/bin/env python
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
import decimal

class PaymentTransaction(object):
	
	def __init__(self, session, consistencyLevel, c_w_id, c_d_id, c_id, payment):
		self.session = session
		self.consistencyLevel = consistencyLevel
		self.c_w_id = int(c_w_id)
		self.c_d_id = int(c_d_id)
		self.c_id = int(c_id)
		self.payment = decimal.Decimal(payment)
		self.initPreparedStmts()
	
	def initPreparedStmts(self):
		self.update_warehouse_query = self.session.prepare("UPDATE warehouse SET w_ytd = ? WHERE w_id = ?")
		self.update_district_query = self.session.prepare("UPDATE district SET d_ytd = ? WHERE d_w_id = ? AND d_id = ?")
		self.select_payment_by_customer_query = self.session.prepare("SELECT c_balance, c_ytd_payment, c_payment_cnt FROM payment_by_customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")
		self.update_payment_by_customer_query = self.session.prepare("UPDATE payment_by_customer SET c_balance = ?, c_ytd_payment = ?,c_payment_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")
		self.output_query = self.session.prepare("SELECT * FROM payment_by_customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")
	
		if self.consistencyLevel == '1' :
			self.update_warehouse_query.consistency_level = ConsistencyLevel.ONE
			self.update_district_query.consistency_level = ConsistencyLevel.ONE
			self.select_payment_by_customer_query.consistency_level = ConsistencyLevel.ONE
			self.update_payment_by_customer_query.consistency_level = ConsistencyLevel.ONE
			self.output_query.consistency_level = ConsistencyLevel.ONE
		else:
			self.update_warehouse_query.consistency_level = ConsistencyLevel.QUORUM
			self.update_district_query.consistency_level = ConsistencyLevel.QUORUM
			self.select_payment_by_customer_query.consistency_level = ConsistencyLevel.QUORUM
			self.update_payment_by_customer_query.consistency_level = ConsistencyLevel.QUORUM
			self.output_query.consistency_level = ConsistencyLevel.QUORUM

	def process(self):
		self.update_warehouse()
		self.update_district()
		self.update_customer()
		self.retrieve_output()

	def update_warehouse(self):
		self.session.execute(self.update_warehouse_query, (self.payment, self.c_w_id))

	def update_district(self):
		self.session.execute(self.update_district_query, (self.payment, self.c_w_id, self.c_d_id))
	
	def update_customer(self):
		customer_attr = self.session.execute(self.select_payment_by_customer_query, (self.c_w_id, self.c_d_id, self.c_id))
		if customer_attr:
			self.c_balance_to_update = customer_attr[0][0] - self.payment
			self.c_ytd_payment_to_update = customer_attr[0][1] + float(self.payment)
			self.c_payment_cnt_to_update = customer_attr[0][2] + 1
			self.session.execute(self.update_payment_by_customer_query,(self.c_balance_to_update, self.c_ytd_payment_to_update, self.c_payment_cnt_to_update, self.c_w_id, self.c_d_id, self.c_id))

	def retrieve_output(self):
		output_attr = self.session.execute(self.output_query, (self.c_w_id, self.c_d_id, self.c_id))
		print ("customer identifier: %d, %d, %d" % (self.c_w_id, self.c_d_id, self.c_id))
		print ("warehouse address: %s, %s, %s, %s, %s" % (output_attr[0].w_address.street_1, output_attr[0].w_address.street_2, output_attr[0].w_address.city, output_attr[0].w_address.state, output_attr[0].w_address.zip))
		print ("district address: %s, %s, %s, %s, %s" % (output_attr[0].d_address.street_1, output_attr[0].d_address.street_2, output_attr[0].d_address.city, output_attr[0].d_address.state, output_attr[0].d_address.zip))
		print ("payment amount: %.2f\n" % self.payment)
