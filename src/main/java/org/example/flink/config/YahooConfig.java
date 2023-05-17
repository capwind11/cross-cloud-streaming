package org.example.flink.config;

import java.io.FileNotFoundException;

/**
 * Encapsulating configuration in once place
 */
public class YahooConfig extends BaseConfig {

    public int numCampaigns;

    public int numAdsPerCampaign;

    /**
     * Creates a config given a Yaml file
     */
    public YahooConfig(String yamlFile) throws FileNotFoundException {
        super(yamlToParameters(yamlFile));
        this.numCampaigns = parameters.getInt("num.campaigns", 100);
        this.numAdsPerCampaign = parameters.getInt("num.ads.per.campaigns", 10);
    }

    /**
     * Create a config directly from the command line arguments
     */
    public static YahooConfig fromArgs(String[] args) throws FileNotFoundException {
        if (args.length < 1) {
            return new YahooConfig("conf/yahoo.yaml");
        } else {
            return new YahooConfig(args[0]);
        }
    }

}
