#!/usr/bin/env python

from cassandra.cluster import Cluster
from time import gmtime, strftime

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
		#self.update_district_stmt = self.session.prepare("UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = ? AND d_id = ?");
		#self.update_item_by_warehouse_district_stmt = self.session.prepare("UPDATE item_by_warehouse_district SET d_next_o_id = d_next_o_id + 1 WHERE w_id = ? AND d_id = ? AND i_id = ?");
		self.select_next_oid_district = self.session.prepare("SELECT d_next_o_id FROM district where d_w_id = ? AND d_id = ?");
		self.update_next_oid_district = self.session.prepare("UPDATE district SET d_next_o_id = ? where d_w_id = ? AND d_id = ?");
		self.insert_orderline = self.session.prepare("INSERT INTO orderline (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D, OL_NUMBER, OL_I_ID, OL_AMOUNT,OL_SUPPLY_W_ID, OL_QUANTITY,OL_DIST_INFO) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		self.select_item_info = self.session.prepare("SELECT i_price, i_name, w_tax, d_tax, s_quantity FROM item_by_warehouse_district where w_id = ? AND i_id = ?");

	def process(self):
		self.updateNextOrderId();	
		self.getAllItemInformation();
		self.insertNewOrderLine();

	# Increment d_next_o_id by 1 in table DISTRICT and ITEM_BY_WAREHOUSE_DISTRICT
	def updateNextOrderId(self):
		rows = self.session.execute(self.select_next_oid_district, (self.w_id, self.d_id));
		self.d_next_o_id = int(rows[0]) + 1;
		self.session.execute(self.update_next_oid_district, (self.d_next_o_id, self.w_id, self.d_id));

	def getAllItemInformation(self):
		self.i_price_list = [0] * self.num_items;
		self.i_name_list = [0] * self.num_items;
		self.w_tax_list = [0] * self.num_items;
		self.d_tax_list = [0] * self.num_items;
		self.s_quantity_list = [0] * self.num_items;

		for i in range(0, self.num_items):
			rows = self.session.execute(self.select_item_info, (self.supplier_w_id_list[i], self.i_id_list[i]));
			self.i_price_list[i] = rows[0][0];
			self.i_name_list[i] = rows[0][1];
			self.w_tax_list[i] = rows[0][2];
			self.d_tax_list[i] = rows[0][3];
			self.s_quantity_list[i] = rows[0][4];

	def insertNewOrderLine(self):
		#Fill in Order data
		self.o_id = self.d_next_o_id;
		self.o_d_id = self.d_id;
		self.o_w_id = self.w_id;
		self.o_c_id = self.c_id;
		self.o_entry_d = strftime("%Y-%m-%d %H:%M:%S", gmtime()); #Not sure format
		self.o_carrier_id = None;
		self.o_ol_cnt = self.num_items;
		self.o_all_local = int(all(x == self.o_w_id for x in self.supplier_w_id_list));

		self.total_amt = 0;

		for i in range(0, self.num_items):
			adjusted_qty = self.s_quantity_list[i] - self.quantity_list[i];
			adjusted_qty = adjusted_qty if adjusted_qty >= 10 else (adjusted_qty+100);

		# Need to update/insert record orderline & delivery_by_customer when is related to order or orderline


