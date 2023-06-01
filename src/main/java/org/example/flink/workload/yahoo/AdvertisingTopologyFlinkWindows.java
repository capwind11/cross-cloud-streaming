package org.example.flink.workload.yahoo;

import edu.umd.cs.findbugs.annotations.Nullable;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.example.flink.config.BaseConfig;
import org.example.flink.config.YahooConfig;
import org.example.flink.workload.BaseWorkload;
import org.example.flink.workload.yahoo.storage.RedisAdCampaignCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class AdvertisingTopologyFlinkWindows implements BaseWorkload {

    private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopologyFlinkWindows.class);

    @Override
    public DataStream<?> createJob(DataStream<?> source, BaseConfig baseConfig, String segment, int breakPoint) throws Exception {

        YahooConfig config = (YahooConfig) baseConfig;

        if ("upstream".equals(segment)) {
            if (breakPoint<1) return source;

            source = ((DataStream<String>)source).flatMap(new DeserializeBolt());
            if (breakPoint<2) return source;

            source = ((DataStream<Tuple>)source).filter(data -> "view".equals(data.getField(4)));
            if (breakPoint<3) return source;

            return source.project(2, 5);
        }

        if ("downstream".equals(segment)) {
            switch (breakPoint) {
                case 0:
                    source = ((DataStream<String>)source).flatMap(new DeserializeBolt());
                case 1:
                    source = ((DataStream<Tuple>)source).filter(data -> "view".equals(data.getField(4)));
                case 2:
                    source = source.project(2, 5);
            }
        }

        DataStream<Tuple2<String, String>> joinedAdImpressions = ((DataStream<Tuple2<String, String>>) source).rebalance()
                .flatMap(new RedisJoinBolt(config)) // campaign_id, event_time
                .assignTimestampsAndWatermarks(new AdTimestampExtractor()); // extract timestamps and generate watermarks from event_time

        WindowedStream<Tuple3<String, String, Long>, Tuple, TimeWindow> windowStream = joinedAdImpressions
                .map(new MapToImpressionCount())
                .keyBy(0) // campaign_id
                .timeWindow(Time.of(config.windowSize, TimeUnit.MILLISECONDS));

        // set a custom trigger
        windowStream.trigger(new EventAndProcessingTimeTrigger());

        // campaign_id, window end time, count
        DataStream<Tuple4<String, String, String, Long>> result =
                windowStream.apply(sumReduceFunction(), sumWindowFunction());

        // write result to redis
        return result;
    }

    private static ReduceFunction<Tuple3<String, String, Long>> sumReduceFunction() {

        return new ReduceFunction<Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> t0, Tuple3<String, String, Long> t1) throws Exception {
                t0.f2 += t1.f2;
                return t0;
            }
        };
    }

    /**
     * Sum - Window function, summing already happened in reduce function
     */
    private static WindowFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, Tuple, TimeWindow> sumWindowFunction() {
        return new WindowFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple keyTuple, TimeWindow window, Iterable<Tuple3<String, String, Long>> values, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
                Iterator<Tuple3<String, String, Long>> valIter = values.iterator();
                Tuple3<String, String, Long> tuple = valIter.next();
                if (valIter.hasNext()) {
                    throw new IllegalStateException("Unexpected");
                }
                Tuple4<String, String, String, Long> tuple4 = new Tuple4<>(tuple.f0, Long.toString(window.getEnd()), tuple.f1, tuple.f2);
                out.collect(tuple4); // collect end time here
            }
        };
    }

    /**
     * Configure Kafka source
     */
    public static FlinkKafkaConsumer<String> kafkaSource(YahooConfig config) {
        return new FlinkKafkaConsumer<>(
                config.kafkaTopic,
                new SimpleStringSchema(),
                config.getParameters().getProperties());
    }

    /**
     * Custom trigger - Fire and purge window when window closes, also fire every 1000 ms.
     */
    private static class EventAndProcessingTimeTrigger extends Trigger<Object, TimeWindow> {

        private static long count = 0;
        private static long timeStamp = System.currentTimeMillis();
        ;

        @Override
        public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            // register system timer only for the first time

//      ValueState<Boolean> firstTimerSet = ctx.getKeyValueState("firstTimerSet", Boolean.class, false);
//      if (!firstTimerSet.value()) {
//        ctx.registerProcessingTimeTimer(System.currentTimeMillis() + 1000L);
//        firstTimerSet.update(true);
//      }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {

            System.out.printf("EventTime trigger interval: %d\n", System.currentTimeMillis() - window.maxTimestamp());
//      timeStamp = System.currentTimeMillis();

            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            // schedule next timer
//      System.out.printf("Processing trigger interval: %d\n", System.currentTimeMillis() - timeStamp);
//      timeStamp = System.currentTimeMillis();

//      ctx.registerProcessingTimeTimer(System.currentTimeMillis() + 1000L);
            return TriggerResult.FIRE;
        }
    }

    /**
     * Parse JSON
     */
    private static class DeserializeBolt implements
            FlatMapFunction<String, Tuple7<String, String, String, String, String, String, String>> {

        transient JSONParser parser = null;

        @Override
        public void flatMap(String input, Collector<Tuple7<String, String, String, String, String, String, String>> out)
                throws Exception {
            if (parser == null) {
                parser = new JSONParser();
            }
            JSONObject obj = (JSONObject) parser.parse(input);

            Tuple7<String, String, String, String, String, String, String> tuple =
                    new Tuple7<>(
                            obj.getAsString("user_id"),
                            obj.getAsString("page_id"),
                            obj.getAsString("ad_id"),
                            obj.getAsString("ad_type"),
                            obj.getAsString("event_type"),
                            obj.getAsString("event_time"),
                            obj.getAsString("ip_address"));
            out.collect(tuple);
        }
    }

    /**
     * Filter out all but "view" events
     */
    public static class EventFilterBolt implements
            FilterFunction<Tuple7<String, String, String, String, String, String, String>> {
        @Override
        public boolean filter(Tuple7<String, String, String, String, String, String, String> tuple) throws Exception {
            return tuple.getField(4).equals("view");
        }
    }

    /**
     * Map ad ids to campaigns using cached data from Redis
     */
    private static final class RedisJoinBolt extends RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, String>> {

        private RedisAdCampaignCache redisAdCampaignCache;
        private YahooConfig config;

        public RedisJoinBolt(YahooConfig config) {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) {
            //initialize jedis
            String redis_host = config.redisHost;
            LOG.info("Opening connection with Jedis to {}", redis_host);
            this.redisAdCampaignCache = new RedisAdCampaignCache(redis_host);
            this.redisAdCampaignCache.prepare();
        }

        @Override
        public void flatMap(Tuple2<String, String> input, Collector<Tuple2<String, String>> out) throws Exception {
            String ad_id = input.getField(0);
            String campaign_id = this.redisAdCampaignCache.execute(ad_id);
            if (campaign_id == null) {
                return;
            }

            Tuple2<String, String> tuple = new Tuple2<>(campaign_id, (String) input.getField(1)); // event_time
            out.collect(tuple);
        }
    }


    /**
     * Generate timestamp and watermarks for data stream
     */
    private static class AdTimestampExtractor implements AssignerWithPeriodicWatermarks<Tuple2<String, String>> {

        long maxTimestampSeen = 0;

        @Override
        public long extractTimestamp(Tuple2<String, String> element, long currentTimestamp) {
            long timestamp = Long.parseLong(element.f1);
            maxTimestampSeen = Math.max(timestamp, maxTimestampSeen);
            return timestamp;
        }

//    @Override
//    public long extractWatermark(Tuple2<String, String> element, long currentTimestamp) {
//      return Long.MIN_VALUE;
//    }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestampSeen - 1L);
        }
    }

    /**
     *
     */
    private static class MapToImpressionCount implements MapFunction<Tuple2<String, String>, Tuple3<String, String, Long>> {
        @Override
        public Tuple3<String, String, Long> map(Tuple2<String, String> t3) throws Exception {
            return new Tuple3<>(t3.f0, t3.f1, 1L);
        }
    }

    /**
     * Sink computed windows to Redis
     */
    private static class RedisResultSink extends RichSinkFunction<Tuple3<String, String, Long>> {
        private Jedis flushJedis;

        private YahooConfig config;

        public RedisResultSink(YahooConfig config) {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            flushJedis = new Jedis(config.redisHost);
        }

        @Override
        public void invoke(Tuple3<String, String, Long> result) throws Exception {
            // set (campaign, count)
            //    flushJedis.hset("campaign-counts", result.f0, Long.toString(result.f2));

            String campaign = result.f0;
            String timestamp = result.f1;
            String windowUUID = getOrCreateWindow(campaign, timestamp);

            flushJedis.hset(windowUUID, "seen_count", Long.toString(result.f2));
            flushJedis.hset(windowUUID, "time_updated", Long.toString(System.currentTimeMillis()));
            flushJedis.lpush("time_updated", Long.toString(System.currentTimeMillis()));
        }

        private String getOrCreateWindow(String campaign, String timestamp) {
            String windowUUID = flushJedis.hmget(campaign, timestamp).get(0);
            if (windowUUID == null) {
                windowUUID = UUID.randomUUID().toString();
                flushJedis.hset(campaign, timestamp, windowUUID);
                getOrCreateWindowList(campaign, timestamp);
            }
            return windowUUID;
        }

        private void getOrCreateWindowList(String campaign, String timestamp) {
            String windowListUUID = flushJedis.hmget(campaign, "windows").get(0);
            if (windowListUUID == null) {
                windowListUUID = UUID.randomUUID().toString();
                flushJedis.hset(campaign, "windows", windowListUUID);
            }
            flushJedis.lpush(windowListUUID, timestamp);
        }

        @Override
        public void close() throws Exception {
            super.close();
            flushJedis.close();
        }
    }

    /**
     * Simplified version of Redis data structure
     */
    private static class RedisResultSinkOptimized extends RichSinkFunction<Tuple4<String, String, String, Long>> {
        private final YahooConfig config;
        private Jedis flushJedis;

        private static long timeStamp = System.currentTimeMillis();

        public RedisResultSinkOptimized(YahooConfig config) {
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
