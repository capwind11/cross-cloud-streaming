package org.example.flink.executor;

import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.example.flink.config.BaseConfig;
import org.example.flink.config.YahooConfig;
import org.example.flink.source.YahooGenerator;
import org.example.flink.workload.yahoo.AdvertisingTopologyFlinkWindows;
import org.example.flink.workload.yahoo.storage.RedisHelper;
import redis.clients.jedis.Jedis;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;

@NoArgsConstructor
public class YahooExecutor extends BaseExecutor {

    public YahooExecutor(int breakPoint, String segment) {
        super(breakPoint, segment);
        typeHintMap.put(1, new TypeHint<Tuple7<String, String, String, String, String, String, String>>() {
        });
        typeHintMap.put(2, new TypeHint<Tuple7<String, String, String, String, String, String, String>>() {
        });
        typeHintMap.put(3, new TypeHint<Tuple2<String, String>>() {
        });
    }

    @Override
    void init(String[] args) throws FileNotFoundException {

        // 初始化配置及环境
        config = YahooConfig.fromArgs(args);
        System.out.println(config);
        // 初始化负载
        workload = new AdvertisingTopologyFlinkWindows();
    }

    @Override
    public void prepareGenerator() throws Exception {

        RichParallelSourceFunction<?> sourceGenerator;
        String sourceName;
        YahooGenerator yahooGenerator = new YahooGenerator((YahooConfig) config);
        sourceGenerator = yahooGenerator;
        sourceName = "YahooGenerator";

        Map<String, List<String>> campaigns = yahooGenerator.getCampaigns();
        RedisHelper redisHelper = new RedisHelper((YahooConfig) config);
        redisHelper.prepareRedis(campaigns);
        redisHelper.writeCampaignFile(campaigns);
        //Choose a source -- Either local generator or Kafka
        source = env.addSource(sourceGenerator, sourceName);
    }

    @Override
    public void output() {

        ((DataStream<Tuple4<String, String, String, Long>>) job).addSink(new RedisResultSinkOptimized(config));
    }

    private static class RedisResultSinkOptimized extends RichSinkFunction<Tuple4<String, String, String, Long>> {
        private final BaseConfig config;
        private Jedis flushJedis;

        private static long timeStamp = System.currentTimeMillis();

        public RedisResultSinkOptimized(BaseConfig config) {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            flushJedis = new Jedis(config.redisHost);
            flushJedis.select(1); // select db 1
        }

        @Override
        public void invoke(Tuple4<String, String, String, Long> result) throws Exception {
            // set campaign id -> (window-timestamp, count)
            System.out.printf("record: %s window latency: %d\n", result.f0, System.currentTimeMillis() - Long.parseLong(result.f1));
            flushJedis.hset(result.f0, result.f1, Long.toString(result.f3));
        }

        @Override
        public void close() throws Exception {
            super.close();
            flushJedis.close();
        }
    }
}
