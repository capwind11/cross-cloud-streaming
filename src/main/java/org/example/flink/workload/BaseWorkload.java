package org.example.flink.workload;

import org.example.flink.common.MyDataStream;
import org.example.flink.config.BaseConfig;

public abstract class BaseWorkload {

    public abstract void createWorkload(BaseConfig config, MyDataStream source) throws Exception;
}
