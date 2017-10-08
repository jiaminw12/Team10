#!/usr/bin/env python

from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from time import gmtime, strftime
from datetime import datetime
import logging, os

#Start logging code
try:
	os.remove(os.getcwd() + "/logs")
except:
	pass

L = logging;
L.basicConfig(filename = 'logs', level = logging.INFO);
#end of logging code


class NewOrderTransaction():

	def __init__(self, session, consistencyLevel, w_id, d_id, c_id, num_items, i_id_list, supplier_w_id_list, quantity_list):
		self.session = session
		self.consistencyLevel = consistencyLevel
		self.w_id = w_id
		self.d_id = d_id
		self.c_id = c_id
		self.num_items = num_items
		self.i_id_list = i_id_list
		self.supplier_w_id_list = supplier_w_id_list
		self.quantity_list = quantity_list

		self.initPreparedStmts()

	def initPreparedStmts(self):

		self.update_stockitem = self.session.prepare("UPDATE stockitem set s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ? where s_w_id = ? and s_i_id = ?");

		self.select_cnt_stockitem = self.session.prepare("SELECT s_order_cnt, s_remote_cnt from stockitem where s_w_id = ? and s_i_id = ?")

		self.update_sqty = self.session.prepare("UPDATE item_by_warehouse_district SET s_quantity = ? WHERE w_id = ? AND i_id = ? and d_id = ?");

		self.select_next_oid_district = self.session.prepare("SELECT d_next_o_id FROM district where d_w_id = ? AND d_id = ?");

		self.update_next_oid_district = self.session.prepare("UPDATE district SET d_next_o_id = ? where d_w_id = ? AND d_id = ?");

		#self.insert_orderline = self.session.prepare("INSERT INTO orderline (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D, OL_NUMBER, OL_I_ID, OL_AMOUNT,OL_SUPPLY_W_ID, OL_QUANTITY,OL_DIST_INFO) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

		self.select_item_info = self.session.prepare("SELECT i_price, i_name,  s_quantity FROM item_by_warehouse_district where w_id = ? AND i_id = ?");

		self.select_tax = self.session.prepare("SELECT w_tax,d_tax from item_by_warehouse_district where w_id = ? and i_id = 1 and d_id = ? ");
		
		self.select_customer_info = self.session.prepare("SELECT c_last, c_credit, c_discount from payment_by_customer where c_w_id = ? and c_d_id = ? and c_id = ?");

		self.insert_orderline = self.session.prepare("INSERT INTO orderline (o_w_id, o_d_id, o_id, o_all_local, o_c_id, o_carrier_id, o_entry_d, o_ol_cnt, ol_number, ol_amount, ol_dist_info, ol_i_id, ol_quantity, ol_supply_w_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

		self.insert_delivery_by_customer = self.session.prepare("INSERT INTO delivery_by_customer (o_w_id, o_d_id, o_id, ol_number, o_c_id, o_carrier_id, o_entry_d, ol_amount, ol_delivery_d, ol_i_id, ol_quantity, ol_supply_w_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");
	
		if self.consistencyLevel == '1' :
			self.update_stockitem.consistency_level = ConsistencyLevel.ONE
			self.select_cnt_stockitem.consistency_level = ConsistencyLevel.ONE
			self.update_sqty.consistency_level = ConsistencyLevel.ONE
			self.select_next_oid_district.consistency_level = ConsistencyLevel.ONE
			self.update_next_oid_district.consistency_level = ConsistencyLevel.ONE
			self.select_item_info.consistency_level = ConsistencyLevel.ONE
			self.select_tax.consistency_level = ConsistencyLevel.ONE
			self.select_customer_info.consistency_level = ConsistencyLevel.ONE
			self.insert_orderline.consistency_level = ConsistencyLevel.ONE
			self.insert_delivery_by_customer.consistency_level = ConsistencyLevel.ONE
		else:
			self.update_stockitem.consistency_level = ConsistencyLevel.QUORUM
			self.select_cnt_stockitem.consistency_level = ConsistencyLevel.QUORUM
			self.update_sqty.consistency_level = ConsistencyLevel.QUORUM
			self.select_next_oid_district.consistency_level = ConsistencyLevel.QUORUM
			self.update_next_oid_district.consistency_level = ConsistencyLevel.QUORUM
			self.select_item_info.consistency_level = ConsistencyLevel.QUORUM
			self.select_tax.consistency_level = ConsistencyLevel.QUORUM
			self.select_customer_info.consistency_level = ConsistencyLevel.QUORUM
			self.insert_orderline.consistency_level = ConsistencyLevel.QUORUM
			self.insert_delivery_by_customer.consistency_level = ConsistencyLevel.QUORUM

	def process(self):
		self.updateNextOrderId();
		self.getTaxInformation();
		self.getCustomerInformation();
		self.getAllItemInformation();
		self.insertNewOrderLine();
		self.printOutput();

	def printOutput(self):
		print("Customer ID(%d,%d,%d), Lastname:%s, Credit:%s, Discount:%.4f" %(self.w_id,self.d_id, self.c_id, self.c_last, self.c_credit, self.c_discount));
	
		print("Warehouse Tax:%.4f, Disctrict Tax:%.4f" %(self.w_tax, self.d_tax));

		print("Order Num: %d, Entry Date: %s" %(self.o_id, self.o_entry_d));

		print("Number of items:%d, Total amount:%.4f" %(self.num_items, self.total_amt));
		
		for i in range(0, self.num_items):
			print("Item Num:%d, Name:%s, Supplier Warehouse:%d, Quantity:%d, OL Amount: %.4f, S_Quantity:%d"%(self.i_id_list[i], self.i_name_list[i], self.supplier_w_id_list[i], self.quantity_list[i], self.itemamt_list[i], self.s_quantity_list[i]));

		
	def updateStockQuantity(self,stock, wid, iid): 	
		for i in range(1,11):
			L.info("Updating stock quantity in ItemByWarehouseDistrict, wid:%d, i_id:%d did:%d"%(wid, iid,i));

			self.session.execute(self.update_sqty, (stock,wid, iid,i));		


	def getCustomerInformation(self):
		L.info("Getting customer information of c_id:%d" %self.c_id);
		rows = self.session.execute(self.select_customer_info, (self.w_id, self.d_id, self.c_id));		
		self.c_last = rows[0].c_last;
		self.c_credit = rows[0].c_credit;
		self.c_discount = rows[0].c_discount;

		L.info("cid:%d => c_last = %s, c_credit = %s, c_discount = %.4f" %(self.c_id, self.c_last, self.c_credit, self.c_discount));

	# Increment d_next_o_id by 1 in table DISTRICT and ITEM_BY_WAREHOUSE_DISTRICT
	def updateNextOrderId(self):
		L.info("Incrementing next_o_id from Table District with wid %d and did %d" %(self.w_id, self.d_id));
		rows = self.session.execute(self.select_next_oid_district, (self.w_id, self.d_id));

		
		self.d_next_o_id = int(rows[0].d_next_o_id) + 1;
		L.info("Incremented next_oid to %d" %self.d_next_o_id);

		self.session.execute(self.update_next_oid_district, (self.d_next_o_id, self.w_id, self.d_id));
		L.info("Updating next_oid=%d of district with wid:%d did:%d" %(self.d_next_o_id, self.w_id, self.d_id));
	
	def getTaxInformation(self):
		L.info("Getting tax information on w_id:%d and d_id:%d" %(self.w_id, self.d_id));
		rows = self.session.execute(self.select_tax, (self.w_id, self.d_id));
		
		self.w_tax = rows[0].w_tax;
		self.d_tax = rows[0].d_tax;

		L.info("For w_id:%d and d_id:%d, w_tax=%.4f and d_tax=%.4f" %(self.w_id, self.d_id, self.w_tax, self.d_tax));
	def getAllItemInformation(self):		
		self.i_price_list = [0] * self.num_items;
		self.i_name_list = [0] * self.num_items;
		self.s_quantity_list = [0] * self.num_items;
		self.itemamt_list = [0] * self.num_items;
		
		L.info("Getting all item information...");

		for i in range(0, self.num_items):
			rows = self.session.execute(self.select_item_info, (self.supplier_w_id_list[i], self.i_id_list[i]));
			self.i_price_list[i] = rows[0][0];
			self.i_name_list[i] = rows[0][1];
			self.s_quantity_list[i] = rows[0][2];
			L.info("w_id:%d i_id:%d price:%d name:%s, stock_qty:%d" %(self.supplier_w_id_list[i], self.i_id_list[i], self.i_price_list[i], self.i_name_list[i], self.s_quantity_list[i]));

	def updateStockItem(self, ytd, _remotecnt,wid, iid):
		rows = self.session.execute(self.select_cnt_stockitem, (wid,iid));
		ordercnt = rows[0].s_order_cnt;
		remotecnt = rows[0].s_remote_cnt;
		L.info("Updating stockitem wid:%d, iid:%d with ytd:%d, remotecnt:%d" %(wid, iid, ytd, remotecnt))
		self.session.execute(self.update_stockitem,(ytd, ordercnt, remotecnt+_remotecnt, wid, iid));

	def insertCustomerByDelivery(self, ol_number, ol_amount, ol_i_id, ol_quantity, ol_supply_w_id):
		self.session.execute(self.insert_delivery_by_customer, (self.w_id, self.d_id, self.o_id, ol_number, self.c_id, self.o_carrier_id, self.o_entry_d, ol_amount, None, ol_i_id, ol_quantity, ol_supply_w_id));
		
	def insertNewOrderLine(self):
		#Fill in Order data
		self.o_id = self.d_next_o_id;
		self.o_d_id = self.d_id;
		self.o_w_id = self.w_id;
		self.o_c_id = self.c_id;
		self.o_entry_d = int(float(datetime.now().strftime("%s.%f"))) * 1000 #strftime("%Y-%m-%d %H:%M:%S", gmtime()); #Not sure format
		self.o_carrier_id = None;
		self.o_ol_cnt = self.num_items;
		self.o_all_local = int(all(x == self.o_w_id for x in self.supplier_w_id_list));

		self.total_amt = 0;

		for i in range(0, self.num_items):
			adjusted_qty = self.s_quantity_list[i] - self.quantity_list[i];
			adjusted_qty = adjusted_qty if adjusted_qty >= 10 else (adjusted_qty+100);
			self.updateStockQuantity(adjusted_qty, self.supplier_w_id_list[i], self.i_id_list[i])
			
			if self.supplier_w_id_list[i] != self.w_id:
				remotecnt = 1;
			else:
				remotecnt = 0;

			self.updateStockItem(self.quantity_list[i], remotecnt, self.supplier_w_id_list[i], self.i_id_list[i]);
			itemamt = self.quantity_list[i] * self.i_price_list[i]
			self.itemamt_list[i] = itemamt;
			self.total_amt += itemamt
			self.total_amt = self.total_amt * (1+self.d_tax + self.w_tax)* (1-self.c_discount)
			self.ol_number = i;
			self.ol_amount = itemamt 
			self.ol_dist_info = "S_DIST_" + str(self.d_id)
			self.ol_i_id = self.i_id_list[i];
			self.ol_quantity = self.quantity_list[i]
			self.ol_supply_w_id = self.supplier_w_id_list[i]
			self.session.execute(self.insert_orderline, (self.o_w_id, self.o_d_id, self.o_id, self.o_all_local, self.o_c_id, self.o_carrier_id, self.o_entry_d, self.o_ol_cnt, self.ol_number, self.ol_amount, self.ol_dist_info, self.ol_i_id, self.ol_quantity, self.ol_supply_w_id));
			self.insertCustomerByDelivery(self.ol_number, self.ol_amount, self.ol_i_id, self.ol_quantity, self.ol_supply_w_id);				
			#L.log("Inserted into orderline wid:%d, did:%d, oid:%d, all_local:%d, cid:%d, carrierid:%d, entry_d:%s, ol_cnt:%d, ol_num:%d, ol_amt: %d, ol_dist_info:%s, ol_i_id:%d, ol_qty:%d, ol_supply_w_id:%d" %(self.o_w_id, self.o_d_id, self.o_id, self.o_all_local, self.o_c_id, self.o_carrier_id, self.o_entry_d, self.o_ol_cnt, self.ol_number, self.ol_amount, self.ol_dist_info, self.ol_i_id, self.ol_quantity, self.ol_supply_w_id));
			L.info("Inserted into orderline wid:%d, did:%d, oid:%d" %(self.o_w_id, self.o_d_id, self.o_id));

# Need to update/insert record orderline & delivery_by_customer when is related to order or orderline
