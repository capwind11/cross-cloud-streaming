package org.example.flink;

import org.example.flink.executor.BaseExecutor;
import org.example.flink.executor.HotitemsExecutor;

public class FlinkRunner {

    public static void main(String args[]) throws Exception {

        BaseExecutor executor = new HotitemsExecutor();
        executor.runJob(new String[]{});
    }
}
