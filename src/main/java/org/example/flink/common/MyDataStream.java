package org.example.flink.common;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyDataStream<T> extends DataStream<T> {

    public MyDataStream(StreamExecutionEnvironment environment, Transformation<T> transformation) {
        super(environment, transformation);
    }
}
