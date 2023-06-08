package org.example.flink.executor;

import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.example.flink.config.HotitemsConfig;
import org.example.flink.source.HotitemsGenerator;
import org.example.flink.workload.hotitems.HotItems;

import java.io.FileNotFoundException;

@NoArgsConstructor
public class HotitemsExecutor extends BaseExecutor {

    public HotitemsExecutor(int breakPoint, String segment) {
        super(breakPoint, segment);
        typeHintMap.put(1, new TypeHint<Tuple5<Long, Long, Integer, String, Long>>() {});
        typeHintMap.put(2, new TypeHint<Tuple5<Long, Long, Integer, String, Long>>() {});
        typeHintMap.put(3, new TypeHint<Tuple2<Long, Long>>() {});
    }

    @Override
    void init(String[] args) throws FileNotFoundException {

        // 初始化配置及环境
        config = HotitemsConfig.fromArgs(args);
        // 初始化负载
        workload = new HotItems();
    }

    @Override
    public void prepareGenerator() throws Exception {

        HotitemsGenerator hotitemsGenerator = new HotitemsGenerator((HotitemsConfig) config);
        RichParallelSourceFunction<?> sourceGenerator = hotitemsGenerator;
        String sourceName = "HotitemsGenerator";

        // Choose a source -- Either local generator or Kafka
        source = env.addSource(sourceGenerator, sourceName);
    }

    @Override
    public void output() {

        job.print();
    }
}
