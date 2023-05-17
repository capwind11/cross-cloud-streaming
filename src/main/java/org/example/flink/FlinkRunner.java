package org.example.flink;

import org.example.flink.executor.BaseExecutor;
import org.example.flink.executor.MarketAnalysisExecutor;

public class FlinkRunner {

    public static void main(String args[]) throws Exception {

        BaseExecutor executor = new MarketAnalysisExecutor();
        executor.runJob(new String[]{});
    }
}
