package org.example.flink.common;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.api.operators.StreamMap;

import java.nio.file.FileStore;

public class MyDataStream<T> {

    private DataStream<T> dataStream;

    public MyDataStream() {

    }

    public MyDataStream(DataStream<T> dataStream) {
        this.dataStream = dataStream;
    }

    public <R> DataStream<R> map(MapFunction<T, R> mapper) {
//        return dataStream;
        return this.dataStream.map(mapper);
    }

    public <R> SingleOutputStreamOperator<R> map(MapFunction<T, R> mapper, TypeInformation<R> outputType) {
        return this.transform("Map", outputType, (OneInputStreamOperator)(new StreamMap((MapFunction)this.clean(mapper))));
    }

    public <R> SingleOutputStreamOperator<R> flatMap(FlatMapFunction<T, R> flatMapper) {
        TypeInformation<R> outType = TypeExtractor.getFlatMapReturnTypes((FlatMapFunction)this.clean(flatMapper), this.getType(), Utils.getCallLocationName(), true);
        return this.flatMap(flatMapper, outType);
    }

    public <R> SingleOutputStreamOperator<R> flatMap(FlatMapFunction<T, R> flatMapper, TypeInformation<R> outputType) {
        return this.transform("Flat Map", outputType, (OneInputStreamOperator)(new StreamFlatMap((FlatMapFunction)this.clean(flatMapper))));
    }
}
