package org.example.flink.config;

import java.io.FileNotFoundException;

/**
 * Encapsulating configuration in once place
 */
public class MarketAnalysisConfig extends BaseConfig {

    public int numUsers;

    public int numAds;

    /**
     * Creates a config given a Yaml file
     */
    public MarketAnalysisConfig(String yamlFile) throws FileNotFoundException {
        super(yamlToParameters(yamlFile));
        this.numUsers = parameters.getInt("num.users", 10000);
        this.numAds = parameters.getInt("num.ads", 1000000);
    }

    /**
     * Create a config directly from the command line arguments
     */
    public static MarketAnalysisConfig fromArgs(String[] args) throws FileNotFoundException {
        if (args.length < 1) {
            return new MarketAnalysisConfig("conf/market_analysis.yaml");
        } else {
            return new MarketAnalysisConfig(args[0]);
        }
    }

}
