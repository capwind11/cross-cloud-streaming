package org.example.flink.source;

import org.example.flink.config.HotPagesConfig;

import java.util.*;

public class HotPagesGenerator extends BaseGenerator<String> {
    private int numUrls;

    private List<String> methods = Arrays.asList("GET", "POST", "HEAD", "PUT");

    private List<String> urls = new ArrayList<>();

    private Random rand = new Random();

    public HotPagesGenerator(HotPagesConfig config) {
        super(config.loadTargetHz, config.timeSliceLengthMs);
        this.numUrls = config.numUrls;
        for (int i = 0; i < numUrls; i++) {
            urls.add(UUID.randomUUID().toString());
        }
    }

    /**
     * Generate a single element
     */
    @Override
    public String generateElement() {
        StringBuilder sb = new StringBuilder();
        sb.setLength(0);
        String ip = UUID.randomUUID().toString();
        String userId = UUID.randomUUID().toString();
        String method = this.methods.get(rand.nextInt(4));
        String url = this.urls.get(rand.nextInt(numUrls));

        sb.append(ip+" ");
        sb.append(userId+" ");
        sb.append(System.currentTimeMillis()+" ");
        sb.append(method+" ");
        sb.append(url);

        return sb.toString();
    }
}
