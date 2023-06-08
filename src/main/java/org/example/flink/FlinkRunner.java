package org.example.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.example.flink.executor.BaseExecutor;
import org.example.flink.executor.HotPagesExecutor;
import org.example.flink.executor.HotitemsExecutor;
import org.example.flink.executor.YahooExecutor;

public class FlinkRunner {

    public static void main(String args[]) throws Exception {


        final ParameterTool params = ParameterTool.fromArgs(args);
        String role = params.get("role", "Downstream");
        int breakpoint = params.getInt("breakpoint", 3);
        String workload = params.get("workload", "hotitems");
        BaseExecutor executor = null;

        if (workload.equals("yahoo")) {
            executor = new YahooExecutor(breakpoint, role);
        } else if (workload.equals("hotitems")) {
            executor = new HotitemsExecutor(breakpoint, role);
        } else if (workload.equals("hotpages")) {
            executor = new HotPagesExecutor(breakpoint, role);
        } else {
            System.out.println("workload not supported");
            System.exit(1);
        }

        if (params.has("rate")) {
            executor.rate = params.getInt("rate");
        }

        executor.runJob(new String[]{});
    }
}
