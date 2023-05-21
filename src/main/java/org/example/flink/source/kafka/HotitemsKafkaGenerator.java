package org.example.flink.source.kafka;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.example.flink.config.HotitemsConfig;
import org.example.flink.source.HotitemsGenerator;

public class HotitemsKafkaGenerator {

    public static void main(String[] args) throws Exception {

        HotitemsConfig hotitemsConfig = HotitemsConfig.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(hotitemsConfig.getParameters());
//    env.setParallelism(10);
        HotitemsGenerator eventGenerator = new HotitemsGenerator(hotitemsConfig);

        DataStream<String> marketData = env.addSource(eventGenerator);

//		marketData.flatMap(new ThroughputLogger<String>(240, 1_000));

        marketData.addSink(new FlinkKafkaProducer<>(
                hotitemsConfig.kafkaTopic,
                new SimpleStringSchema(),
                hotitemsConfig.getParameters().getProperties()
        ));
        env.disableOperatorChaining();

        env.execute("hotitems data generator " + hotitemsConfig.getParameters().toMap().toString());
    }

}
