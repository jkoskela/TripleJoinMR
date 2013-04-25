#!/usr/bin/env bash
# This script will execute a transitive closure flow using one of the classes from join.jar
# The only options for ClassName are BinaryJoinTC or TripleJoinTC
# Input should be edges of the format "u,v", one per line.

badargs()
{
   echo usage: sh tc.sh ClassName input output numReducer
   echo  *ClassName should be either BinaryJoinTC or TripleJoinTC
   exit
}

if [ $# -ne 4 ] || [ "$1" != BinaryJoinTC -a "$1" != TripleJoinTC ]
then
   badargs
fi

set +o posix
class=$1
input=$2
output=$3
reducers=$4
result=1

bin/hadoop fs -rmr $input $output
bin/hadoop fs -put $input $input
rm $output temp

while [ $result -ne 0 ]
do
   bin/hadoop jar join.jar $class $input $output $reducers 
   rm $output
   bin/hadoop fs -getmerge $output $output
   diff <(sort -u $output) <(sort -u temp)
   result=$?
   cp $output temp
   bin/hadoop fs -rmr $input $output
   bin/hadoop fs -put $output $input
done


