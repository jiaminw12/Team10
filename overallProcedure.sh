#!/bin/bash

# The following script allows user to benchmark Cassandra performance
# Enter arguments to select the data to benchmark:
# bash benchmark.sh arg0

# arg0 can have the following values:
#		arg0: Number of clients - 10, 20, 40

declare -r DATA_FOLDER="data-files"
declare -r XACT_FOLDER="xact-files"

cd ~/Team10

echo -ne "Checking whether data and xact folder exist...\n"
if [ -d $DATA_FOLDER ];
then
	echo -ne "yes \n"
else
	echo -ne "Downloading 4224-project-files...\n"
	wget http://www.comp.nus.edu.sg/~cs4224/4224-project-files.zip
	unzip 4224-project-files.zip

	cd 4224-project-files
	mv data-files ~/Team10/
	mv xact-files ~/Team10/
	cd ..
	rm -Rf 4224-project-files
	rm -Rf 4224-project-files.zip
	echo -ne "Done...\n"
	
	nodetool status | awk '/^(U|D)(N|L|J|M)/{print $2}' > nodeList.txt
fi

IFS=$'\n' read -d '' -r -a lines < nodeList.txt

# Load all data to all tables
bash bulkload.sh
# Copy all data from all tables
bash ~/Team10/copyTableScript/copyAllTableData.sh
cd ~/Team10


# 10 clients
echo -ne "Executing 10 clients for CONSISTENCY LEVEL ONE .... \n"
cqlsh ${lines[0]}  -e "CONSISTENCY ONE"
bash ~/Team10/benchmark/benchmark10.sh 1 > benchmarkResult1001.txt
cp -a ~/Team10/log ~/Team10/log1001
echo -ne "Done... \n"

bash bulkload02.sh
cd ~/Team10
echo -ne "Executing 10 clients for CONSISTENCY LEVEL QUORUM .... \n"
cqlsh ${lines[0]}  -e "CONSISTENCY QUORUM"
bash ~/Team10/benchmark/benchmark10.sh 2 > benchmarkResult1002.txt
cp -a ~/Team10/log ~/Team10/log1002
echo -ne "Done... \n"


# 20 clients
bash bulkload02.sh
cd ~/Team10
echo -ne "Executing 20 clients for CONSISTENCY LEVEL ONE .... \n"
cqlsh ${lines[0]}  -e "CONSISTENCY ONE"
bash ~/Team10/benchmark/benchmark20.sh 1 > benchmarkResult2001.txt
cp -a ~/Team10/log ~/Team10/log2001
echo -ne "Done... \n"

bash bulkload02.sh
cd ~/Team10
echo -ne "Executing 20 clients for CONSISTENCY LEVEL QUORUM .... \n"
cqlsh ${lines[0]}  -e "CONSISTENCY QUORUM"
bash ~/Team10/benchmark/benchmark20.sh 2 > benchmarkResult2002.txt
cp -a ~/Team10/log ~/Team10/log2002
echo -ne "Done... \n"


# 40 clients
bash bulkload02.sh
cd ~/Team10
echo -ne "Executing 40 clients for CONSISTENCY LEVEL ONE .... \n"
cqlsh ${lines[0]}  -e "CONSISTENCY ONE"
bash ~/Team10/benchmark/benchmark40.sh 1 > benchmarkResult4001.txt
cp -a ~/Team10/log ~/Team10/log4001
echo -ne "Done... \n"

bash bulkload02.sh
cd ~/Team10
echo -ne "Executing 40 clients for CONSISTENCY LEVEL QUORUM .... \n"
cqlsh ${lines[0]}  -e "CONSISTENCY QUORUM"
bash ~/Team10/benchmark/benchmark40.sh 2 > benchmarkResult4002.txt
cp -a ~/Team10/log ~/Team10/log4002
echo -ne "Done... \n"
rm -rf log


