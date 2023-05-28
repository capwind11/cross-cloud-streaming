package org.example.flink.executor;

import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.example.flink.config.HotitemsConfig;
import org.example.flink.source.HotitemsGenerator;
import org.example.flink.workload.hotitems.HotItems;

import java.io.FileNotFoundException;

@NoArgsConstructor
public class HotitemsExecutor extends BaseExecutor {

    public HotitemsExecutor(int breakPoint, String segment) {
        super(breakPoint, segment);
    }

    @Override
    void init(String[] args) throws FileNotFoundException {

        // 初始化配置及环境
        config = HotitemsConfig.fromArgs(args);
        // 初始化负载
        workload = new HotItems();
    }

    @Override
    void prepareSource() throws Exception {

        RichParallelSourceFunction<String> sourceGenerator;
        String sourceName;
        if ("source".equals(segment)) {
            HotitemsGenerator hotitemsGenerator = new HotitemsGenerator((HotitemsConfig) config);
            sourceGenerator = hotitemsGenerator;
            sourceName = "HotitemsGenerator";
        } else {
            String kafkaTopic = config.kafkaTopic;
            if ("downstream".equals(segment)) {
                kafkaTopic += breakPoint;
            }
            sourceGenerator = new FlinkKafkaConsumer<>(
                    kafkaTopic,
                    new SimpleStringSchema(),
                    config.getParameters().getProperties()
            );
            sourceName = kafkaTopic;
        }
        // Choose a source -- Either local generator or Kafka
        source = env.addSource(sourceGenerator, sourceName);
    }

    @Override
    void addSink() {
        if ("source".equals(segment)) {
            ((DataStream<String>) job).addSink(new FlinkKafkaProducer<>(
                    config.kafkaTopic,
                    new SimpleStringSchema(),
                    config.getParameters().getProperties()
            )).name(config.kafkaTopic);

        } else if ("upstream".equals(segment)) {
                String kafkaTopic = config.kafkaTopic += breakPoint;
            ((DataStream<String>) job).addSink(new FlinkKafkaProducer<>(
                    kafkaTopic,
                    new SimpleStringSchema(),
                    config.getParameters().getProperties()
            )).name(kafkaTopic);
        } else {
            job.print();
        }
    }
}
