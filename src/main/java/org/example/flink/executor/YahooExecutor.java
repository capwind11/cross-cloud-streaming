package org.example.flink.executor;

import lombok.Getter;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.example.flink.config.YahooConfig;
import org.example.flink.source.YahooGenerator;
import org.example.flink.workload.yahoo.storage.RedisHelper;
import org.example.flink.workload.yahoo.AdvertisingTopologyFlinkWindows;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;

@Getter
public class YahooExecutor extends BaseExecutor {

    @Override
    public void init(String[] args) throws FileNotFoundException {

        // 初始化配置及环境
        config = YahooConfig.fromArgs(args);
        // 初始化负载
        workload = new AdvertisingTopologyFlinkWindows();
    }

    @Override
    void prepareSource() {
        // Choose a source -- Either local generator or Kafka
        RichParallelSourceFunction<String> sourceGenerator;
        String sourceName;
        if (config.useLocalEventGenerator) {
            YahooGenerator eventGenerator = new YahooGenerator((YahooConfig) config);
            sourceGenerator = eventGenerator;
            sourceName = "EventGenerator";

            Map<String, List<String>> campaigns = eventGenerator.getCampaigns();
            RedisHelper redisHelper = new RedisHelper((YahooConfig) config);
            redisHelper.prepareRedis(campaigns);
            redisHelper.writeCampaignFile(campaigns);
        } else {
            sourceGenerator = new FlinkKafkaConsumer<>(
                    config.kafkaTopic,
                    new SimpleStringSchema(),
                    config.getParameters().getProperties());;
            sourceName = "Kafka";
        }

        source = env.addSource(sourceGenerator, sourceName).disableChaining();
    }
}
