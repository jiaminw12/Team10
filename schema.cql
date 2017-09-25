DROP MATERIALIZED VIEW IF EXISTS team10.Top_Balance;
DROP TABLE IF EXISTS team10.Warehouse;
DROP TABLE IF EXISTS team10.District;

DROP MATERIALIZED VIEW IF EXISTS team10.Delivery_by_Customer;
DROP TABLE IF EXISTS team10.OrderLine;

DROP TABLE IF EXISTS team10.StockItem;
DROP TABLE IF EXISTS team10.Payment_by_Customer;
DROP TABLE IF EXISTS team10.Item_by_Warehouse_District;

DROP KEYSPACE IF EXISTS team10;

CREATE KEYSPACE team10 WITH replication =
{'class':'SimpleStrategy','replication_factor':3};

CREATE TYPE team10.address (
    STREET_1 text,
    STREET_2 text,
    CITY text,
    STATE text,
    ZIP text
);

CREATE TABLE team10.Warehouse (
    W_ID int,
    W_NAME text,
    W_ADDRESS frozen<address>,
    W_TAX decimal,
    W_YTD decimal,
    PRIMARY KEY (W_ID)
);

CREATE TABLE team10.District (
    D_W_ID int,
    D_ID int,
    D_NAME text,
    D_ADDRESS frozen<address>,
    D_TAX decimal,
    D_YTD decimal,
    D_NEXT_O_ID int,
    PRIMARY KEY (D_W_ID, D_ID)
);

CREATE TABLE team10.OrderLine (
    O_W_ID int,
    O_D_ID int,
    O_ID int,
    O_C_ID int,
    O_CARRIER_ID int,
    O_OL_CNT decimal,
    O_ALL_LOCAL decimal,
    O_ENTRY_D timestamp,
    OL_NUMBER int,
    OL_I_ID int,
    OL_DELIVERY_D timestamp,
    OL_AMOUNT decimal,
    OL_SUPPLY_W_ID int,
    OL_QUANTITY decimal,
    OL_DIST_INFO text,
    PRIMARY KEY ((O_W_ID, O_D_ID), O_ID, OL_NUMBER)
) WITH CLUSTERING ORDER BY (O_ID ASC);

CREATE MATERIALIZED VIEW IF NOT EXISTS team10.Delivery_by_Customer AS
    SELECT O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, OL_DELIVERY_D, OL_AMOUNT, O_ENTRY_D, OL_QUANTITY, OL_SUPPLY_W_ID, OL_I_ID
    FROM team10.OrderLine
    WHERE O_W_ID IS NOT NULL
    AND O_D_ID IS NOT NULL
    AND O_ID IS NOT NULL
    AND OL_NUMBER IS NOT NULL
    PRIMARY KEY (O_W_ID, O_D_ID, O_ID, OL_NUMBER)
WITH CLUSTERING ORDER BY (O_ID DESC);

CREATE TABLE team10.StockItem (
    S_W_ID int,
    S_I_ID int,
    S_YTD decimal,
    S_ORDER_CNT int,
    S_REMOTE_CNT int,
    S_DIST_01 text,
    S_DIST_02 text,
    S_DIST_03 text,
    S_DIST_04 text,
    S_DIST_05 text,
    S_DIST_06 text,
    S_DIST_07 text,
    S_DIST_08 text,
    S_DIST_09 text,
    S_DIST_10 text,
    S_DATA text,
    I_PRICE decimal,
    I_IM_ID int,
    I_DATA text,
    PRIMARY KEY ((S_W_ID), S_I_ID)
);

CREATE TABLE team10.Payment_by_Customer (
    C_W_ID int,
    C_D_ID int,
    C_ID int,
    C_FIRST text,
    C_MIDDLE text,
    C_LAST text,
    C_ADDRESS frozen<address>,
    C_PHONE text,
    C_SINCE timestamp,
    C_CREDIT text,
    C_CREDIT_LIM decimal,
    C_DISCOUNT decimal,
    C_BALANCE decimal,
    C_YTD_PAYMENT float,
    C_PAYMENT_CNT int,
    C_DELIVERY_CNT int,
    C_DATA text,
    W_ADDRESS frozen<address>,
    D_ADDRESS frozen<address>,
    PRIMARY KEY((C_W_ID, C_D_ID), C_ID)
);

CREATE MATERIALIZED VIEW IF NOT EXISTS team10.Top_Balance AS
    SELECT C_ID, C_W_ID, C_D_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST, W_ADDRESS, D_ADDRESS
    FROM team10.Payment_by_Customer
    WHERE C_ID IS NOT NULL
    AND C_W_ID IS NOT NULL
    AND C_D_ID IS NOT NULL
    AND C_BALANCE IS NOT NULL
    PRIMARY KEY (C_ID, C_W_ID, C_D_ID, C_BALANCE)
WITH CLUSTERING ORDER BY (C_BALANCE DESC);

CREATE TABLE team10.Item_by_Warehouse_District (
    W_ID int,
    I_ID int,
    D_ID int,
    I_PRICE decimal,
    I_NAME text,
    W_TAX decimal,
    D_TAX decimal,
    D_NEXT_O_ID int,
    S_QUANTITY decimal,
    PRIMARY KEY (W_ID, I_ID, D_ID)
) WITH CLUSTERING ORDER BY (I_ID ASC, D_ID ASC);

