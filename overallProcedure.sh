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
fi


# ONE
# The consistency level defaults to ONE for all write and read operations.
cqlsh -e "CONSISTENCY ONE"

bash bulkload.sh

bash benchmark.sh 10 1 > benchmark1001.txt
cp -a ~/Team10/log ~/Team10/log1001

bash benchmark.sh 20 1 > benchmark2001.txt
cp -a ~/Team10/log ~/Team10/log2001

bash benchmark.sh 40 1 > benchmark4001.txt
cp -a ~/Team10/log ~/Team10/log4001


# QUORUM
# The consistency level changes to QUORUM for all write and read operations.
cqlsh -e "CONSISTENCY QUORUM"

bash bulkload.sh

bash benchmark.sh 10 2 > benchmark1002.txt
cp -a ~/Team10/log ~/Team10/log1002

bash benchmark.sh 20 2 > benchmark2002.txt
cp -a ~/Team10/log ~/Team10/log2002

bash benchmark.sh 40 2 > benchmark4002.txt
cp -a ~/Team10/log ~/Team10/log4002

rm -rf log

