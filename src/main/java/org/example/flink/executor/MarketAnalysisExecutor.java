package org.example.flink.executor;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.example.flink.common.MyDataStream;
import org.example.flink.config.MarketAnalysisConfig;
import org.example.flink.source.MarketAnalysisGenerator;
import org.example.flink.workload.market_analysis.AdStatisticsByProvince;

import java.io.FileNotFoundException;

public class MarketAnalysisExecutor extends BaseExecutor {
    @Override
    void init(String[] args) throws FileNotFoundException {
        // 初始化配置及环境
        config = MarketAnalysisConfig.fromArgs(args);
        // 初始化负载
        workload = new AdStatisticsByProvince();
    }

    @Override
    void prepareSource() throws Exception {
        // Choose a source -- Either local generator or Kafka
        RichParallelSourceFunction<String> sourceGenerator;
        String sourceName;
        if (config.useLocalEventGenerator) {
            MarketAnalysisGenerator eventGenerator = new MarketAnalysisGenerator((MarketAnalysisConfig) config);
            sourceGenerator = eventGenerator;
            sourceName = "EventGenerator";
        } else {
            sourceGenerator = new FlinkKafkaConsumer<>(
                    config.kafkaTopic,
                    new SimpleStringSchema(),
                    config.getParameters().getProperties());;
            sourceName = "Kafka";
        }

        source = new MyDataStream(env.addSource(sourceGenerator, sourceName).disableChaining());
    }
}
