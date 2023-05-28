package org.example.flink;

import org.example.flink.executor.BaseExecutor;
import org.example.flink.executor.HotitemsExecutor;

public class FlinkRunnerDownStream {

    public static void main(String args[]) throws Exception {

        BaseExecutor executor = new HotitemsExecutor(3, "downstream");
        executor.runJob(new String[]{});
    }
}
