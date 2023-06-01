package org.example.flink.executor;

import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.config.BaseConfig;
import org.example.flink.workload.BaseWorkload;

import java.io.FileNotFoundException;

@NoArgsConstructor
public abstract class BaseExecutor {


    // execution setting
    public int breakPoint;

    public String segment;

    public DataStream<?> source;

    public BaseWorkload workload;

    public StreamExecutionEnvironment env;

    public BaseConfig config;

    public DataStream<?> job;

    public BaseExecutor(int breakPoint, String segment) {
        this.breakPoint = breakPoint;
        this.segment = segment;
    }

    public void runJob(String[] args) throws Exception {

        // Implemented By Child
        init(args);

        Configuration configuration = new Configuration();

        // TODO 本地是否可以共用一个端口
        // 本地执行设置端口
//        if ("local".equals(config.env)) {
//            int port = 8082;
//            if ("upstream".equals(segment)) {
//                port = 8083;
//            }  else if ("downstream".equals(segment)) {
//                port = 8084;
//            }
//            configuration.setInteger(RestOptions.PORT, port);
//            configuration.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
//        }

        // 初始化环境
        env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.getConfig().setGlobalJobParameters(config.getParameters());

//        if (config.checkpointsEnabled) {
//            env.enableCheckpointing(config.checkpointInterval);
//        }

        // Implemented By Child
        prepareSource();

        if (!"source".equals(segment)) {
            job = workload.createJob(source, config, segment, breakPoint);
        } else {
            job = source;
        }
        addSink();
        env.disableOperatorChaining();
        env.execute();
    }

    abstract void addSink();

    abstract void init(String[] args) throws FileNotFoundException;

    abstract void prepareSource() throws Exception;

}
