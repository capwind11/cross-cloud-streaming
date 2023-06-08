package org.example.flink.workload.network;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.flink.config.BaseConfig;
import org.example.flink.workload.BaseWorkload;
import org.example.flink.workload.network.beans.PageViewCount;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.regex.Pattern;

public class HotPages implements BaseWorkload {

    @Override
    public DataStream<?> createJob(DataStream<?> source, BaseConfig baseConfig, String segment, int breakPoint) throws Exception {

        if ("upstream".equals(segment)) {
            if (breakPoint < 1) return source;

            source = ((DataStream<String>) source).map(
                    new MapFunction<String, Tuple5<String, String, Long, String, String>>() {
                        @Override
                        public Tuple5<String, String, Long, String, String> map(String line) throws Exception {
                            String[] fields = line.split(" ");

                            return new Tuple5(fields[0], fields[1], new Long(fields[2]), fields[3], fields[4]);
                        }
                    }
            );
            if (breakPoint < 2) return source;

            source = ((DataStream<Tuple>) source).filter(data -> "GET".equals(data.getField(3))).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.LONG, Types.STRING, Types.STRING));    // 过滤get请求
            if (breakPoint < 3) return source;

            // 只保留 timestamp和url
            source = source.project(2, 4);
            if (breakPoint < 4) return source;

            return ((DataStream<Tuple>) source).filter(data -> {
                String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                return Pattern.matches(regex, data.getField(1));
            });
        }

        if ("downstream".equals(segment)) {
            switch (breakPoint) {
                case 0:
                    source = ((DataStream<String>) source).map(
                            new MapFunction<String, Tuple5<String, String, Long, String, String>>() {
                                @Override
                                public Tuple5<String, String, Long, String, String> map(String line) throws Exception {
                                    String[] fields = line.split(" ");

                                    return new Tuple5(fields[0], fields[1], new Long(fields[2]), fields[3], fields[4]);
                                }
                            }
                    );
                case 1:
                    source = ((DataStream<Tuple>) source).filter(data -> "GET".equals(data.getField(3)));    // 过滤get请求
                case 2:
                    source = source.project(2, 4);
                case 3:
                    source = ((DataStream<Tuple>) source).filter(data -> {
                        String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                        return Pattern.matches(regex, data.getField(1));
                    });
            }
        }

        SingleOutputStreamOperator<PageViewCount> windowAggStream = ((DataStream<Tuple2<Long, String>>) source).assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<Tuple2<Long, String>>(Time.seconds(1)) {
                            @Override
                            public long extractTimestamp(Tuple2<Long, String> element) {
                                return element.getField(0);
                            }
                        }).keyBy(1)    // 按照url分组
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .aggregate(new PageCountAgg(), new PageCountResult());

        windowAggStream.print("agg");

        // 收集同一窗口count数据，排序输出
        DataStream<String> resultStream = windowAggStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPages(3));

        return resultStream;
    }


    // 自定义预聚合函数
    public static class PageCountAgg implements AggregateFunction<Tuple2<Long, String>, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Long, String> value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // 实现自定义的窗口函数
    public static class PageCountResult implements WindowFunction<Long, PageViewCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple url, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(url.getField(0), window.getEnd(), input.iterator().next()));
        }
    }

    // 实现自定义的处理函数
    public static class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String> {
        private Integer topSize;

        public TopNHotPages(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义状态，保存当前所有PageViewCount到Map中
//        ListState<PageViewCount> pageViewCountListState;
        MapState<String, Long> pageViewCountMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
//            pageViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("page-count-list", PageViewCount.class));
            pageViewCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("page-count-map", String.class, Long.class));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<String> out) throws Exception {
//            pageViewCountListState.add(value);
            pageViewCountMapState.put(value.getUrl(), value.getCount());
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
            // 注册一个1分钟之后的定时器，用来清空状态
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60 * 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 先判断是否到了窗口关闭清理时间，如果是，直接清空状态返回
            if (timestamp == ctx.getCurrentKey() + 60 * 1000L) {
                pageViewCountMapState.clear();
                return;
            }

            ArrayList<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(pageViewCountMapState.entries());

            pageViewCounts.sort(new Comparator<Map.Entry<String, Long>>() {
                @Override
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    if (o1.getValue() > o2.getValue())
                        return -1;
                    else if (o1.getValue() < o2.getValue())
                        return 1;
                    else
                        return 0;
                }
            });

            // 格式化成String输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("===================================\n");
            resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");

            // 遍历列表，取top n输出
            for (int i = 0; i < Math.min(topSize, pageViewCounts.size()); i++) {
                Map.Entry<String, Long> currentItemViewCount = pageViewCounts.get(i);
                resultBuilder.append("NO ").append(i + 1).append(":")
                        .append(" 页面URL = ").append(currentItemViewCount.getKey())
                        .append(" 浏览量 = ").append(currentItemViewCount.getValue())
                        .append("\n");
            }
            resultBuilder.append("===============================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());

//            pageViewCountListState.clear();
        }
    }
}
