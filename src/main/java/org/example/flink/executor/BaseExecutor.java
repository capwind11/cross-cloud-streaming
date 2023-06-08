package org.example.flink.executor;

import lombok.NoArgsConstructor;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.example.flink.common.FlinkRebalancePartitioner;
import org.example.flink.config.BaseConfig;
import org.example.flink.workload.BaseWorkload;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@NoArgsConstructor
public abstract class BaseExecutor {


    // execution setting
    public int breakPoint;

    public int rate = -1;
    public String segment;

    public DataStream<?> source;

    public BaseWorkload workload;

    public StreamExecutionEnvironment env;

    public BaseConfig config;
    public Map<Integer, TypeHint<?>> typeHintMap = new HashMap();
    public DataStream<?> job;

    public String jobName;

    public BaseExecutor(int breakPoint, String segment) {
        this.breakPoint = breakPoint;
        this.jobName = segment+this.getClass().getSimpleName();
        this.segment = segment.toLowerCase();
        typeHintMap.put(0, new TypeHint<String>() {});
    }

    public void runJob(String[] args) throws Exception {

        // Implemented By Child
        init(args);

        if (rate!=-1) {
            config.loadTargetHz = rate;
        }
        Configuration configuration = new Configuration();

        // TODO 本地是否可以共用一个端口
        // 本地执行设置端口
//        if ("local".equals(config.env)) {
//            int port = 8082;
//            if ("upstream".equals(segment)) {
//                port = 8083;
//            }  else if ("downstream".equals(segment)) {
//                port = 8084;
//            }
//            configuration.setInteger(RestOptions.PORT, port);
//            configuration.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
//        }

        // 初始化环境
        env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.getConfig().setGlobalJobParameters(config.getParameters());

//        if (config.checkpointsEnabled) {
//            env.enableCheckpointing(config.checkpointInterval);
//        }

        // Implemented By Child
        prepareSource();

        if (!"source".equals(segment)) {
            job = workload.createJob(source, config, segment, breakPoint);
        } else {
            job = source;
        }
        addSink();

        env.disableOperatorChaining();
        env.execute(jobName);
    }

    public void addSink() {
        if ("source".equals(segment)) {
            ((DataStream<String>) job).addSink(new FlinkKafkaProducer<>(
                    config.kafkaTopic,
                    new SimpleStringSchema(),
                    config.getParameters().getProperties(),
                    Optional.of(new FlinkRebalancePartitioner<>())
            )).name(config.kafkaTopic);
        } else if ("upstream".equals(segment)) {
            String kafkaTopic = config.kafkaTopic += breakPoint;
            ((DataStream<Tuple>) job).addSink(new FlinkKafkaProducer<>(
                    kafkaTopic,
                    new TypeInformationSerializationSchema<>((TypeInformation<Tuple>) TypeInformation.of(typeHintMap.get(breakPoint)), env.getConfig()),
                    config.getParameters().getProperties(),
                    Optional.of(new FlinkRebalancePartitioner<>())
            )).name(kafkaTopic);
        } else {
            output();
        }
    }

    public abstract void output();

    abstract void init(String[] args) throws FileNotFoundException;

    public void prepareSource() throws Exception {
        if ("source".equals(segment)) {
            prepareGenerator();
            return;
        }
        RichParallelSourceFunction<?> sourceGenerator;
        String sourceName;
        if ("upstream".equals(segment)) {
            String kafkaTopic = config.kafkaTopic;
            sourceGenerator = new FlinkKafkaConsumer<>(kafkaTopic, new SimpleStringSchema(), config.getParameters().getProperties());
            sourceName = kafkaTopic;
        } else {
            String kafkaTopic = config.kafkaTopic;
            DeserializationSchema schema = null;
            if (breakPoint!=0) {
                kafkaTopic += breakPoint;
                schema = new TypeInformationSerializationSchema<>(TypeInformation.of(typeHintMap.get(breakPoint)), env.getConfig());

            } else {
                schema = new SimpleStringSchema();
            }
            sourceGenerator = new FlinkKafkaConsumer<>(kafkaTopic, schema, config.getParameters().getProperties());
            sourceName = kafkaTopic;
        }
        source = env.addSource(sourceGenerator, sourceName);
    }

    public abstract void prepareGenerator() throws Exception;


}
