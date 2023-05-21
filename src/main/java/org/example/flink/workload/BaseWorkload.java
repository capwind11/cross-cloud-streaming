package org.example.flink.workload;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.example.flink.config.BaseConfig;

public abstract class BaseWorkload {

    public abstract void createWorkload(BaseConfig config, DataStream<String> source) throws Exception;
}
