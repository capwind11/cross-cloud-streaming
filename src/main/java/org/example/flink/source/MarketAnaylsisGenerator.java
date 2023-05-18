package org.example.flink.source;


import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.example.flink.config.MarketAnalysisConfig;
import org.example.flink.config.YahooConfig;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * A data generator source
 */
@Getter
@Setter
public class MarketAnaylsisGenerator extends BaseGenerator<String> {
    private Map<String, List<String>> provinces;

    public MarketAnaylsisGenerator(MarketAnalysisConfig config) throws Exception {
        super(config.loadTargetHz, config.timeSliceLengthMs);
        provinces = generateProvince();
    }

    /**
     * Generate a single element
     */
    @Override
    public String generateElement() {
//        this.userId = userId;
//        this.adId = adId;
//        this.province = province;
//        this.city = city;
//        this.timestamp = timestamp;
        if (adsIdx == ads.size()) {
            adsIdx = 0;
        }
        if (eventsIdx == eventTypes.length) {
            eventsIdx = 0;
        }
        StringBuilder sb = new StringBuilder();
        sb.setLength(0);
        sb.append("{\"user_id\":\"");
        sb.append(pageID);
        sb.append("\",\"page_id\":\"");
        sb.append(userID);
        sb.append("\",\"ad_id\":\"");
        sb.append(ads.get(adsIdx++));
        sb.append("\",\"ad_type\":\"");
        sb.append("banner78"); // value is immediately discarded. The original generator would put a string with 38/5 = 7.6 chars. We put 8.
        sb.append("\",\"event_type\":\"");
        sb.append("view");
//    sb.append(eventTypes[eventsIdx++]);
        sb.append("\",\"event_time\":\"");
        sb.append(System.currentTimeMillis());
        sb.append("\",\"ip_address\":\"1.2.3.4\"}");

        return sb.toString();
    }

    private Map<String, List<String>> generateProvince() throws Exception {

        FileInputStream fileInputStream = new FileInputStream("province.json");

        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
        BufferedReader reader = new BufferedReader(inputStreamReader);
        String jsonString = "";
        String tempString = null;
        while (true) {
            try {
                if (!((tempString = reader.readLine()) != null)) break;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            jsonString += tempString;
        }
        Map<String, List<String>> provinces = (Map<String, List<String>>) JSONObject.parse(jsonString);
        for (provinces.entrySet())
        return provinces;
    }
}
