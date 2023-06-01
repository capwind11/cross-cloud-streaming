package org.example.flink;

import org.example.flink.executor.BaseExecutor;
import org.example.flink.executor.YahooExecutor;

public class FlinkRunnerDownStream {

    public static void main(String args[]) throws Exception {

        BaseExecutor executor = new YahooExecutor(3, "downstream");
        executor.runJob(new String[]{});
    }
}
