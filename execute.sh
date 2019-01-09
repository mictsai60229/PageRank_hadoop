#!/bin/bash

# Do not uncomment these lines to directly execute the script
# Modify the path to fit your need before using this script
#hdfs dfs -rm -r /user/TA/CalculateAverage/Output/
#hadoop jar CalculateAverage.jar calculateAverage.CalculateAverage /user/shared/CalculateAverage/Input /user/TA/CalculateAverage/Output
#hdfs dfs -cat /user/TA/CalculateAverage/Output/part-*


if [[ "$1" == "" ]]; then
    input=100M
else
    input=$1
fi
if [[ "$2" == "" ]]; then
    iter=3
else
    iter=$2
fi

INPUT_FILE=/user/ta/PageRank/input-$input
OUTPUT_FILE=PageRank/PageRankOut
JAR=Page_Rank.jar

hdfs dfs -rm -r PageRank/*
hadoop jar $JAR page_rank.Page_Rank $INPUT_FILE $OUTPUT_FILE $iter

hdfs dfs -getmerge $OUTPUT_FILE pagerank_$input.out
