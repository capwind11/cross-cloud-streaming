package org.example.flink.executor;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.example.flink.config.MarketAnalysisConfig;
import org.example.flink.workload.market_analysis.AdStatisticsByProvince;

import java.io.FileNotFoundException;
import java.util.Properties;

public class MarketAnalysisExecutor extends BaseExecutor {
    @Override
    void init(String[] args) throws FileNotFoundException {
        // 初始化配置及环境
        config = MarketAnalysisConfig.fromArgs(args);
        // 初始化负载
        workload = new AdStatisticsByProvince();
    }

    @Override
    void prepareSource() {

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000);
        env.setParallelism(4);

        //Todo 2.准备kafka连接参数
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "127.0.0.1:9092");
        prop.setProperty("group.id", "consumer-group");
        prop.setProperty("auto.offset.reset", "earliest");
        String topic = "adClickLog";

        //Todo 3.创建kafka数据源
        FlinkKafkaConsumer<String> flinkKafkaConsumer = (FlinkKafkaConsumer<String>)
                new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),prop);
        flinkKafkaConsumer.setStartFromEarliest();
        //Todo 4.指定消费者参数
        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true);//默认为true
        //Todo 从最早的数据开始消费
        flinkKafkaConsumer.setStartFromEarliest();

//        DataStream<String> inputStream = env.readTextFile("/Users/zhangyang/experiment/benchmark/flink_second_understand/FlinkStudy/src/main/java/com/threeknowbigdata/flink/market_analysis/data/AdClickLog.csv");
        source = env.addSource(flinkKafkaConsumer);
    }
}
