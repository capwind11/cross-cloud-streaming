package org.example.flink.config;

import org.apache.flink.api.java.utils.ParameterTool;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class BaseConfig implements Serializable {

    // Kafka
    public String kafkaTopic;

    // Akka
    public String akkaZookeeperQuorum;
    public String akkaZookeeperPath;

    // Load Generator
    public int loadTargetHz;
    public int timeSliceLengthMs;
    public boolean useLocalEventGenerator;

    // Redis
    public String redisHost;
    public int redisDb;
    public boolean redisFlush;
    public int numRedisThreads;

    // Application
    public long windowSize;

    // Flink
    public long checkpointInterval;
    public boolean checkpointsEnabled;
    public String checkpointUri;
    public boolean checkpointToUri;

    // The raw parameters
    public ParameterTool parameters;

    // execution setting
    public String env;
    public boolean upstream;

    public boolean downstream;

    public int breakPoint;


    /**
     * Get the parameters
     */
    public ParameterTool getParameters(){
        return this.parameters;
    }

    public BaseConfig(ParameterTool parameterTool) {
        this.parameters = parameterTool;

        // load generator
        this.loadTargetHz = parameterTool.getInt("load.target.hz", 400_000);
        this.timeSliceLengthMs = parameterTool.getInt("load.time.slice.length.ms", 100);
        this.useLocalEventGenerator = parameters.has("use.local.event.generator");

        // Kafka
        this.kafkaTopic = parameterTool.getRequired("kafka.topic");

        // Redis
        this.redisHost = parameterTool.get("redis.host", "localhost");
        this.redisDb = parameterTool.getInt("redis.db", 0);
        this.redisFlush = parameterTool.has("redis.flush");
        this.numRedisThreads = parameterTool.getInt("redis.threads", 20);

        // Akka
        this.akkaZookeeperQuorum = parameterTool.get("akka.zookeeper.quorum", "localhost");
        this.akkaZookeeperPath = parameterTool.get("akka.zookeeper.path", "/akkaQuery");

        // Application
        this.windowSize = parameterTool.getLong("window.size", 10_000);

        // Flink
        this.checkpointInterval = parameterTool.getLong("flink.checkpoint.interval", 0);
        this.checkpointsEnabled = checkpointInterval > 0;
        this.checkpointUri = parameterTool.get("flink.checkpoint.uri", "");
        this.checkpointToUri = checkpointUri.length() > 0;

        // Env
        this.env = parameterTool.get("env", "local");
        this.upstream = parameterTool.has("upstream");
        this.downstream = parameterTool.has("downstream");
    }

    public static ParameterTool yamlToParameters(String yamlFile) throws FileNotFoundException {
        // load yaml file
        Yaml yml = new Yaml(new SafeConstructor());
        Map<String, String> ymlMap = (Map) yml.load(new FileInputStream(yamlFile));

        String kafkaZookeeperConnect = getZookeeperServers(ymlMap, String.valueOf(ymlMap.get("kafka.zookeeper.path")));
        String akkaZookeeperQuorum = getZookeeperServers(ymlMap, "");

        // We need to add these values as "parameters"
        // -- This is a bit of a hack but the Kafka consumers and producers
        //    expect these values to be there
        ymlMap.put("zookeeper.connect", kafkaZookeeperConnect); // set ZK connect for Kafka
        ymlMap.put("bootstrap.servers", getKafkaBrokers(ymlMap));
        ymlMap.put("akka.zookeeper.quorum", akkaZookeeperQuorum);
        ymlMap.put("auto.offset.reset", "latest");
        ymlMap.put("group.id", UUID.randomUUID().toString());

        // Convert everything to strings
        for (Map.Entry e : ymlMap.entrySet()) {
            {
                e.setValue(e.getValue().toString());
            }
        }
        return ParameterTool.fromMap(ymlMap);
    }

    public static String getZookeeperServers(Map conf, String zkPath) {
        if(!conf.containsKey("zookeeper.servers")) {
            throw new IllegalArgumentException("Not zookeeper servers found!");
        }
        return listOfStringToString((List<String>) conf.get("zookeeper.servers"), String.valueOf(conf.get("zookeeper.port")), zkPath);
    }

    public static String getKafkaBrokers(Map conf) {
        if(!conf.containsKey("kafka.brokers")) {
            throw new IllegalArgumentException("No kafka brokers found!");
        }
        if(!conf.containsKey("kafka.port")) {
            throw new IllegalArgumentException("No kafka port found!");
        }
        return listOfStringToString((List<String>) conf.get("kafka.brokers"), String.valueOf(conf.get("kafka.port")), "");
    }

    public static String listOfStringToString(List<String> list, String port, String path) {
        String val = "";
        for(int i=0; i<list.size(); i++) {
            val += list.get(i) + ":" + port + path;
            if(i < list.size()-1) {
                val += ",";
            }
        }
        return val;
    }
}
