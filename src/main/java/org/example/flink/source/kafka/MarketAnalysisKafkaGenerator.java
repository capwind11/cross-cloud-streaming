package org.example.flink.source.kafka;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.flink.config.MarketAnalysisConfig;
import org.example.flink.source.MarketAnalysisGenerator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

public class MarketAnalysisKafkaGenerator {

    public static void main(String[] args) throws Exception {

        MarketAnalysisConfig marketAnalysisConfig = MarketAnalysisConfig.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(marketAnalysisConfig.getParameters());
//    env.setParallelism(10);
        MarketAnalysisGenerator eventGenerator = new MarketAnalysisGenerator(marketAnalysisConfig);

        DataStream<String> marketData = env.addSource(eventGenerator);

//		marketData.flatMap(new ThroughputLogger<String>(240, 1_000));

        marketData.addSink(new FlinkKafkaProducer<>(
                marketAnalysisConfig.kafkaTopic,
                new SimpleStringSchema(),
                marketAnalysisConfig.getParameters().getProperties()
        ));
        env.disableOperatorChaining();

        env.execute("Ad Impressions data generator " + marketAnalysisConfig.getParameters().toMap().toString());
    }

    public static void main1(String[] args) throws Exception {
        KafkaProducer<String, String> producer;
        Properties props = new Properties();
        //指定代理服务器的地址
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer(props);

        int count = 0;

        for(int i = 0; i < 10000; i++){
          // BufferedReader bufferedReader = new BufferedReader(new FileReader(new File("G:\\pythonWorkSpace\\lightgbm_code\\Flink sql\\application_train.csv")));
          // BufferedReader bufferedReader = new BufferedReader(new FileReader(new File("/Users/zhangyang/experiment/benchmark/flink_second_understand/FlinkStudy/data/lightgbm_test.csv")));
          // BufferedReader bufferedReader = new BufferedReader(new FileReader(new File("data/Home_Credit_0.7968_307507_797_test_data_notitle.csv")));
            BufferedReader bufferedReader = new BufferedReader(new FileReader(new File("/Users/zhangyang/experiment/benchmark/flink_second_understand/FlinkStudy/src/main/java/com/threeknowbigdata/flink/market_analysis/data/AdClickLog.csv")));
            String line = null;

            while((line = bufferedReader.readLine()) != null){
                producer.send(new ProducerRecord<>("adClickLog", null, line));
                count++;
                if(count % 1000 == 0 ){
                    System.out.println(count);
                }

            }
            bufferedReader.close();
        }

        producer.close();
    }
}
