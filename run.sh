#!/bin/bash

set -x

FLINK=/Users/zhangyang/opt/flink-1.16.1/bin/flink
JAR_PATH=/Users/zhangyang/paper/flink-benchmark/target/flink-bencnmark-1.0-SNAPSHOT.jar
### dataflow configuration ###
WORKLOAD="hotpages"
BREAKPOINT=3
QUERY_CLASS="org.example.flink.FlinkRunner"
#$FLINK run -d -m localhost:8081 -p 1 -c ${QUERY_CLASS} $JAR_PATH --role Source --workload $WORKLOAD
#$FLINK run -d -m localhost:8081 -p 1 -c ${QUERY_CLASS} $JAR_PATH --role Upstream --breakpoint $BREAKPOINT --workload $WORKLOAD
$FLINK run -d -m localhost:8082 -p 4 -c ${QUERY_CLASS} $JAR_PATH --role Downstream --breakpoint $BREAKPOINT --workload $WORKLOAD

