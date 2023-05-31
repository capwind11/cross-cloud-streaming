package org.example.flink;

import org.example.flink.executor.BaseExecutor;
import org.example.flink.executor.HotPagesExecutor;

public class FlinkRunnerDownStream {

    public static void main(String args[]) throws Exception {

        BaseExecutor executor = new HotPagesExecutor(3, "downstream");
        executor.runJob(new String[]{});
    }
}
