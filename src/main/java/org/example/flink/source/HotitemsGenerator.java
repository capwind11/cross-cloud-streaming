package org.example.flink.source;


import lombok.Getter;
import lombok.Setter;
import org.example.flink.config.HotitemsConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * A data generator source
 */
@Getter
@Setter
public class HotitemsGenerator extends BaseGenerator<String> {

    private int numCategories;
    private int numUsers;

    private List<String> behaviors = Arrays.asList("buy", "cart", "fav", "pv");
    
    private int numItems;

    private Random rand = new Random();

    public HotitemsGenerator(HotitemsConfig config) throws Exception {
        super(config.loadTargetHz, config.timeSliceLengthMs);
        numUsers = config.numUsers;
        numItems = config.numItems;
        numCategories = config.numCategories;
    }

    /**
     * Generate a single element
     */
    @Override
    public String generateElement() {

//        this.userId = userId;
//        this.itemId = itemId;
//        this.categoryId = categoryId;
//        this.behavior = behavior;
//        this.timestamp = timestamp;
        StringBuilder sb = new StringBuilder();
        sb.setLength(0);
        int userId = rand.nextInt(numUsers);
        int itemId = rand.nextInt(numItems);
        int categoryId = rand.nextInt(numCategories);
        String behavior = behaviors.get(rand.nextInt(4));

        sb.append(userId+",");
        sb.append(itemId+",");
        sb.append(categoryId+",");
        sb.append(behavior+",");
        sb.append(System.currentTimeMillis());

        return sb.toString();
    }
}
