package org.example.flink.executor;

import lombok.NoArgsConstructor;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.example.flink.config.HotitemsConfig;
import org.example.flink.source.HotitemsGenerator;
import org.example.flink.workload.hotitems.HotItems;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

@NoArgsConstructor
public class HotitemsExecutor extends BaseExecutor {

    private static Map<Integer, TypeHint<? extends Tuple>> typeHintMap = new HashMap();

    static {
        typeHintMap.put(1, new TypeHint<Tuple5<Long, Long, Integer, String, Long>>() {});
        typeHintMap.put(2, new TypeHint<Tuple5<Long, Long, Integer, String, Long>>() {});
        typeHintMap.put(3, new TypeHint<Tuple2<Long, Long>>() {});
    }

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

        RichParallelSourceFunction<?> sourceGenerator;
        String sourceName;
        if ("source".equals(segment)) {
            HotitemsGenerator hotitemsGenerator = new HotitemsGenerator((HotitemsConfig) config);
            sourceGenerator = hotitemsGenerator;
            sourceName = "HotitemsGenerator";
        } else if ("upstream".equals(segment)) {
            String kafkaTopic = config.kafkaTopic;
            sourceGenerator = new FlinkKafkaConsumer<>(kafkaTopic, new SimpleStringSchema(), config.getParameters().getProperties());
            sourceName = kafkaTopic;
        } else {
            String kafkaTopic = config.kafkaTopic;
            if ("downstream".equals(segment)) {
                kafkaTopic += breakPoint;
            }
            sourceGenerator = new FlinkKafkaConsumer<>(kafkaTopic, new TypeInformationSerializationSchema<>(TypeInformation.of(typeHintMap.get(breakPoint)), env.getConfig()), config.getParameters().getProperties());
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
            ((DataStream<Tuple>) job).addSink(new FlinkKafkaProducer<>(
                    kafkaTopic,
                    new TypeInformationSerializationSchema<>((TypeInformation<Tuple>) TypeInformation.of(typeHintMap.get(breakPoint)), env.getConfig()),
                    config.getParameters().getProperties()
            )).name(kafkaTopic);
        } else {
            job.print();
        }
    }
}
