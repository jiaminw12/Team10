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
	echo -ne "yes"
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

bash benchmark.sh 10 > benchmark1001.txt
bash benchmark.sh 20 > benchmark2001.txt
bash benchmark.sh 40 > benchmark4001.txt


# QUORUM
# The consistency level changes to QUORUM for all write and read operations.
cqlsh -e "CONSISTENCY QUORUM"

bash bulkload.sh

bash benchmark.sh 10 > benchmark1002.txt
bash benchmark.sh 20 > benchmark2002.txt
bash benchmark.sh 40 > benchmark4002.txt

