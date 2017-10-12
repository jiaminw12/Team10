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

nodetool status | awk '/^(U|D)(N|L|J|M)/{print $2}' > nodeList.txt

IFS=$'\n' read -d '' -r -a lines < nodeList.txt

bash bulkload.sh
#bash ~/Team10/copyTableScript/copyAllTableData.sh

#cd ~/Team10

# ONE
# The consistency level defaults to ONE for all write and read operations.
#cqlsh ${lines[0]}  -e "CONSISTENCY ONE"

#bash ~/Team10/benchmark/benchmark10.sh 1 > benchmark1001.txt
#cp -a ~/Team10/log ~/Team10/log1001

#bash ~/Team10/benchmark/benchmark20.sh 1 > benchmark2001.txt
#cp -a ~/Team10/log ~/Team10/log2001

#bash ~/Team10/benchmark/benchmark40.sh 1 > benchmark4001.txt
#cp -a ~/Team10/log ~/Team10/log4001

#bash bulkload02.sh
#bash ~/Team10/copyTableScript/copyAllTableData.sh

# QUORUM
# The consistency level defaults to QUORUM for all write and read operations.
#cqlsh ${lines[0]}  -e "CONSISTENCY QUORUM"

#bash ~/Team10/benchmark/benchmark10.sh 2 > benchmark1002.txt
#cp -a ~/Team10/log ~/Team10/log1002

#bash ~/Team10/benchmark/benchmark20.sh 2 > benchmark2002.txt
#cp -a ~/Team10/log ~/Team10/log2002

#bash ~/Team10/benchmark/benchmark40.sh 2 > benchmark4002.txt
#cp -a ~/Team10/log ~/Team10/log4002


