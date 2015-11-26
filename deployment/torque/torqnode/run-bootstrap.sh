#!/bin/sh
echo "Loading bootstrapping file from  $1"
wget -O bs.sh $1
chmod +x bs.sh 
echo "Running bootstrapping file ... "
./bs.sh $2 $3 $4 $5 $6 $7 $8 $9 
echo "bootstrapping done !"
