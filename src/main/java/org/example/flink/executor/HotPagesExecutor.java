package org.example.flink.executor;

import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.example.flink.config.HotPagesConfig;
import org.example.flink.source.HotPagesGenerator;
import org.example.flink.workload.network.HotPages;

import java.io.FileNotFoundException;

@NoArgsConstructor
public class HotPagesExecutor extends BaseExecutor {

    public HotPagesExecutor(int breakPoint, String segment) {
        super(breakPoint, segment);
        typeHintMap.put(1, new TypeHint<Tuple5<String, String, Long, String, String>>() {
        });
        typeHintMap.put(2, new TypeHint<Tuple5<String, String, Long, String, String>>() {
        });
        typeHintMap.put(3, new TypeHint<Tuple2<Long, String>>() {
        });
        typeHintMap.put(4, new TypeHint<Tuple2<Long, String>>() {
        });
    }

    @Override
    void init(String[] args) throws FileNotFoundException {

        // 初始化配置及环境
        config = HotPagesConfig.fromArgs(args);
        // 初始化负载
        workload = new HotPages();
    }

    @Override
    public void prepareGenerator() throws Exception {

        RichParallelSourceFunction<?> sourceGenerator;
        String sourceName;
        HotPagesGenerator hotpagesGenerator = new HotPagesGenerator((HotPagesConfig) config);
        sourceGenerator = hotpagesGenerator;
        sourceName = "HotPagesGenerator";
        // Choose a source -- Either local generator or Kafka
        source = env.addSource(sourceGenerator, sourceName);
    }

    @Override
    public void output() {
        job.print();
    }
}
