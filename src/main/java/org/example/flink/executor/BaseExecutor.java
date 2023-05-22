package org.example.flink.executor;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.example.flink.common.MyDataStream;
import org.example.flink.config.BaseConfig;
import org.example.flink.workload.BaseWorkload;

import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.List;

public abstract class BaseExecutor {

    public BaseWorkload workload;

    public StreamExecutionEnvironment env;

    public MyDataStream source;

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

        workload.createWorkload(config, source);
        reconfigureGraph();
        env.execute();
    }

    abstract void init(String[] args) throws FileNotFoundException;

    abstract void prepareSource() throws Exception;

    public void reconfigureGraph() {
        List<Transformation<?>> transformations = env.getTransformations();
        StreamGraph streamGraph = env.getStreamGraph(false);
        StreamGraph newStreamGraph = env.getStreamGraph(false);
        newStreamGraph.clear();
        for (Transformation<?> transformation: transformations) {

            Collection<StreamNode> streamNodes = streamGraph.getStreamNodes();
            System.out.println("stop");
        }

    }
}
