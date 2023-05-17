package org.example.flink.workload;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.example.flink.config.BaseConfig;

public interface BaseWorkload {

    public void  createJob(BaseConfig config, DataStream<String> source) throws Exception;
}
