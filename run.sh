#!/bin/bash

set -x
FLINK=/Users/zhangyang/opt/flink-1.16.1/bin/flink
JAR_PATH=/Users/zhangyang/paper/flink-benchmark/target/flink-bencnmark-1.0-SNAPSHOT.jar
### dataflow configuration ###
QUERY_CLASS="org.example.flink.FlinkRunner"

#$FLINK run -d --class org.example.flink.common.SubmitTool $JAR_PATH --q $QUERY_CLASS --address $ADDRESS --f $JAR_PATH "--mode normal -p 5 --source-rate 120000"
$FLINK run -d -m localhost:8081 -p 1 -c ${QUERY_CLASS}Source $JAR_PATH
$FLINK run -d -m localhost:8081 -p 1 -c ${QUERY_CLASS}Upstream $JAR_PATH
$FLINK run -d -m localhost:8082 -p 2 -c ${QUERY_CLASS}DownStream $JAR_PATH

