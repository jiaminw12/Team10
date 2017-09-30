from cassandra.cluster import Cluster
import decimal

class PaymentTransaction(object):

  def __init__(self, session, c_w_id, c_d_id, c_id, payment):
    self.session = session
    self.c_w_id = int(c_w_id)
    self.c_d_id = int(c_d_id)
    self.c_id = int(c_id)
    self.payment = decimal.Decimal(payment)
    self.initPreparedStmts()

  def initPreparedStmts(self):
    self.update_warehouse_query = self.session.prepare(
        "UPDATE team10.warehouse SET w_ytd = ? WHERE w_id = ?")
    self.update_district_query = self.session.prepare(
        "UPDATE team10.district SET d_ytd = ? WHERE d_w_id = ? AND d_id = ?")
    self.select_payment_by_customer_query = self.session.prepare(
        "SELECT c_balance, c_ytd_payment, c_payment_cnt FROM "
        "team10.payment_by_customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")
    self.update_payment_by_customer_query = self.session.prepare(
        "UPDATE team10.payment_by_customer SET c_balance = ?,"
        "c_ytd_payment = ?,c_payment_cnt = ? "
        "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")
    self.output_query = self.session.prepare(
        "SELECT * FROM team10.payment_by_customer "
        "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")

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
    customer_attr = self.session.execute(
        self.select_payment_by_customer_query, (self.c_w_id, self.c_d_id, self.c_id))
    self.c_balance_to_update = customer_attr[0][0] - self.payment
    self.c_ytd_payment_to_update = customer_attr[0][1] + float(self.payment)
    self.c_payment_cnt_to_update = customer_attr[0][2] + 1
    self.session.execute(
        self.update_payment_by_customer_query,
        (self.c_balance_to_update, self.c_ytd_payment_to_update, self.c_payment_cnt_to_update, self.c_w_id, self.c_d_id, self.c_id))

  def retrieve_output(self):
    output_attr = self.session.execute(self.output_query, (self.c_w_id, self.c_d_id, self.c_id))
    # TODO(cf) Need to complete output in the default format.
    print ("customer identifier: %d, %d, %d" % (self.c_w_id, self.c_d_id, self.c_id))
    print ("warehouse address: %s" % output_attr[0].w_address)
    print ("district address")
    print ("payment payment")

