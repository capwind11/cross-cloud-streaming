package org.example.flink.config;

import java.io.FileNotFoundException;

/**
 * Encapsulating configuration in once place
 */
public class HotitemsConfig extends BaseConfig {

    public int numUsers;

    public int numCategories;

    public int numItems;

    /**
     * Creates a config given a Yaml file
     */
    public HotitemsConfig(String yamlFile) throws FileNotFoundException {
        super(yamlToParameters(yamlFile));
        this.numUsers = parameters.getInt("num.users", 10000);
        this.numItems = parameters.getInt("num.items", 1000000);
        this.numCategories = parameters.getInt("num.categories", 100);
    }

    /**
     * Create a config directly from the command line arguments
     */
    public static HotitemsConfig fromArgs(String[] args) throws FileNotFoundException {
        if (args.length < 1) {
            return new HotitemsConfig("conf/hotitems.yaml");
        } else {
            return new HotitemsConfig(args[0]);
        }
    }

}
