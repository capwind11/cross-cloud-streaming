WORK_DIR = "/Users/zhangyang/experiment/CReSt/flink-bencnmark/"
JAR_PATH = "target/flink-bencnmark-1.0-SNAPSHOT.jar"

FLINK_PATH = "/Users/zhangyang/opt/flink-1.16.1/bin/flink"

# PACKAGE = "org.example.flink.nexmark.v2.queries"
PACKAGE = "org.example.flink.v2"
QUERY_CLASS = [
    "nexmark.Query1",
    "nexmark.Query2",
    "nexmark.Query3",
    "nexmark.Query3Stateful",
    "nexmark.Query5",
    # "Query5Keyed",
    "nexmark.Query8",
    # "Query8Keyed",
    # "Query8Ori",
    "nexmark.Query11",
    "wordcount.RateControlledWordCount",
    "wordcount.StatefulWordCount",
    "wordcount.TwoInputsWordCount"
]
