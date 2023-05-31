package org.example.flink.config;

import java.io.FileNotFoundException;

/**
 * Encapsulating configuration in once place
 */
public class HotPagesConfig extends BaseConfig {

    public int numUrls;

    /**
     * Creates a config given a Yaml file
     */
    public HotPagesConfig(String yamlFile) throws FileNotFoundException {
        super(yamlToParameters(yamlFile));
        this.numUrls = parameters.getInt("numUrls",10000);
    }

    /**
     * Create a config directly from the command line arguments
     */
    public static HotPagesConfig fromArgs(String[] args) throws FileNotFoundException {
        if (args.length < 1) {
            return new HotPagesConfig("conf/hotpages.yaml");
        } else {
            return new HotPagesConfig(args[0]);
        }
    }

}
