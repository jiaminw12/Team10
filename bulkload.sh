#!/bin/bash

# The following script allows user to retrieve data and transactions
# and load the data into the database by running the following:
# bash bulkload.sh arg0

# The arguments can have the following values:
#		arg0: Number of clients - 10, 20, 40

# Conditions:
#       Project (Team10-Cassandra) is in home directory
#		cqlsh is within /temp/Cassandra/bin directory

declare -r DATA_FOLDER="data-files"
declare -r XACT_FOLDER="xact-files"

# Create data folder only if not exist
echo -ne "Checking whether data-files and xact-files exist..."
if [ -d $DATA_FOLDER && -d XACT_FOLDER]
then
    echo "yes"
else
    echo "Start downloading data-files and xact-files..."
    wget http://www.comp.nus.edu.sg/~cs4224/4224-project-files.zip
    unzip 4224-project-files.zip
    echo "data-files and xact-files are created successfully\n"
fi


# Bulk load data
echo -ne "Loading WAREHOUSE, DISTRICT, ORDER-LINE, STOCK-ITEM, PAYMENT_BY_CUSTOMER ITEM_BY_WAREHOUSE_DISTRICT and DELIVERY_BY_CUSTOMER data into Cassandra..."
cd /temp/Cassandra/bin
./cqlsh -f ~/Team10-Cassandra/schema.cql
./cqlsh -f ~/Team10-Cassandra/schema-script.cql


cd ~/Team10-Cassandra/app
echo -ne "\nLoading item data into Cassandra.."
# use python here to load data
#mvn -q install &>/dev/null
#mvn -q compile &>/dev/null
#mvn -q exec:java -Dexec.mainClass="database.Denormalize" -Dexec.args="$1"
./Bulkload.py
