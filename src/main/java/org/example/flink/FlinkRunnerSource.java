package org.example.flink;

import org.example.flink.executor.BaseExecutor;
import org.example.flink.executor.YahooExecutor;

public class FlinkRunnerSource {

    public static void main(String args[]) throws Exception {

        BaseExecutor executor = new YahooExecutor(0, "source");
        executor.runJob(new String[]{});
    }
}
