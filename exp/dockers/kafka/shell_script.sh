kafka-topics.sh --zookeeper zookeeper:2181 --describe --topic words
kafka-topics.sh --zookeeper zookeeper:2181 --describe --topic ads
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group flink_benchmark
kafka-topics.sh --zookeeper zookeeper:2181 --create --topic ads --partitions 10 --replication-factor 1