package org.example.flink;

import org.example.flink.executor.BaseExecutor;
import org.example.flink.executor.HotitemsExecutor;

public class FlinkRunnerSource {

    public static void main(String args[]) throws Exception {

        BaseExecutor executor = new HotitemsExecutor(0, "source");
        executor.runJob(new String[]{});
    }
}
