package org.example.flink.executor;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.config.BaseConfig;
import org.example.flink.workload.BaseWorkload;

import java.io.FileNotFoundException;

public abstract class BaseExecutor {

    public BaseWorkload workload;

    public StreamExecutionEnvironment env;

    public DataStream<String> source;

    public BaseConfig config;

    public void runJob(String[] args) throws Exception {

        // Implemented By Child
        init(args);

        Configuration configuration = new Configuration();

        // 本地执行设置端口
        if ("local".equals(config.env)) {
            configuration.setInteger(RestOptions.PORT, 8082);
            configuration.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        }

        // 初始化环境
        env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.getConfig().setGlobalJobParameters(config.getParameters());

        if (config.checkpointsEnabled) {
            env.enableCheckpointing(config.checkpointInterval);
        }

        // Implemented By Child
        prepareSource();

        workload.createJob(config, source);
        env.execute();
    }

    abstract void init(String[] args) throws FileNotFoundException;

    abstract void prepareSource();
}
