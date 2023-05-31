package org.example.flink.workload;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.example.flink.config.BaseConfig;

public interface BaseWorkload {

    DataStream<?> createJob(DataStream<?> source, BaseConfig baseConfig, String segment, int breakPoint) throws Exception;
}
