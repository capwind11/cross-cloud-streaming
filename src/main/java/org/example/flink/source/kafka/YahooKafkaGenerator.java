package org.example.flink.source.kafka;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.example.flink.source.YahooGenerator;
import org.example.flink.workload.yahoo.storage.RedisHelper;
import org.example.flink.config.YahooConfig;

import java.util.List;
import java.util.Map;

/**
 * Distributed Data Generator for AdImpression Events.
 *
 *
 * (by default) we generate 100 campaigns, with 10 ads each.
 * We write those 1000 ads into Redis, with ad_is --> campaign_id
 *
 *
 *
 *
 */
public class YahooKafkaGenerator {

	public static void main(String[] args) throws Exception {

    YahooConfig yahooConfig = YahooConfig.fromArgs(args);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(yahooConfig.getParameters());
//    env.setParallelism(10);
    YahooGenerator eventGenerator = new YahooGenerator(yahooConfig);

    Map<String, List<String>> campaigns = eventGenerator.getCampaigns();
    RedisHelper redisHelper = new RedisHelper(yahooConfig);
    redisHelper.prepareRedis(campaigns);
    redisHelper.writeCampaignFile(campaigns);

    DataStream<String> adImpressions = env.addSource(eventGenerator);

//		adImpressions.flatMap(new ThroughputLogger<String>(240, 1_000));

    adImpressions.addSink(new FlinkKafkaProducer<>(
      yahooConfig.kafkaTopic,
      new SimpleStringSchema(),
      yahooConfig.getParameters().getProperties()
    ));
    env.disableOperatorChaining();

		env.execute("Ad Impressions data generator " + yahooConfig.getParameters().toMap().toString());
	}
}
