package org.example.flink.executor;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.example.flink.config.HotitemsConfig;
import org.example.flink.config.MarketAnalysisConfig;
import org.example.flink.source.MarketAnalysisGenerator;
import org.example.flink.workload.hotitems.HotItems;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class HotitemsExecutor extends BaseExecutor {

    @Override
    void init(String[] args) throws FileNotFoundException {
        // 初始化配置及环境
        config = HotitemsConfig.fromArgs(args);
        // 初始化负载
        workload = new HotItems();
    }

    @Override
    void prepareSource() throws Exception {

    }

    void prepareSource(int operatorId) throws Exception {
        RichParallelSourceFunction<String> sourceGenerator;
        String sourceName;
        if (operatorId == 0 && config.useLocalEventGenerator) {
            MarketAnalysisGenerator eventGenerator = new MarketAnalysisGenerator((MarketAnalysisConfig) config);
            sourceGenerator = eventGenerator;
            sourceName = "EventGenerator";
        } else {
            String kafkaTopic = config.kafkaTopic;
            if (operatorId > 0) {
                kafkaTopic += "-" + operatorId;
            }
            sourceGenerator = new FlinkKafkaConsumer<>(
                    kafkaTopic,
                    new SimpleStringSchema(),
                    config.getParameters().getProperties());
            sourceName = "Kafka";
        }
        // Choose a source -- Either local generator or Kafka

         map = env.addSource(sourceGenerator, sourceName).map(
                new MapFunction<String, Tuple5<Long, Long, Integer, String, Long>>() {
                    @Override
                    public Tuple5<Long, Long, Integer, String, Long> map(String line) throws Exception {
                        String[] fields = line.split(",");

                        return new Tuple5<Long, Long, Integer, String, Long>(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                    }
                }
        );
    }

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
        prepareSource(0);

        workload.createJob(config, map);
        List<Transformation<?>> transformations = env.getTransformations();
        env.close();

        List<String> upStream = Arrays.asList("Map", "Filter", "Projection");
        List<String> downStream = Arrays.asList("Timestamps/Watermarks", "SlidingEventTimeWindows", "KeyedProcess", "Print to Std. Out");

        String upStreamEnd = "";
        String downStreamStart = "";

        StreamExecutionEnvironment upStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        StreamExecutionEnvironment downStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        for (Transformation transformation : transformations) {
            if (upStream.contains(transformation.getName())) {
                upStreamEnv.addOperator(transformation);
            } else {
                downStreamEnv.addOperator(transformation);
            }
        }

        List<String> test = Collections.singletonList("test");
        downStreamEnv.generateStreamGraph(transformations);
//        upStreamEnv.execute();
        downStreamEnv.execute();
    }
}
