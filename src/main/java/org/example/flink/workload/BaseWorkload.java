package org.example.flink.workload;

import org.apache.flink.streaming.api.datastream.DataStream;

public interface BaseWorkload {
    DataStream<?> createJob(DataStream<?> source, String segment, int breakPoint) throws Exception;
}
