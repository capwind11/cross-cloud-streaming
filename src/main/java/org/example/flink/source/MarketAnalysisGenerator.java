package org.example.flink.source;


import com.alibaba.fastjson.JSONObject;
import javafx.util.Pair;
import lombok.Getter;
import lombok.Setter;
import org.example.flink.config.MarketAnalysisConfig;

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
public class MarketAnalysisGenerator extends BaseGenerator<String> {
    private List<Pair<String, String>> city2province;

    private int numUsers;

    private int numAds;

    private Random rand = new Random();

    public MarketAnalysisGenerator(MarketAnalysisConfig config) throws Exception {
        super(config.loadTargetHz, config.timeSliceLengthMs);
        city2province = generateProvince();
        numUsers = config.numUsers;
        numAds = config.numAds;

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
        StringBuilder sb = new StringBuilder();
        sb.setLength(0);
        int userId = rand.nextInt(numUsers);
        int adId = rand.nextInt(numAds);
        int cityId = rand.nextInt(city2province.size());

        sb.append(userId+",");
        sb.append(adId+",");
        sb.append(city2province.get(cityId).getKey()+",");
        sb.append(city2province.get(cityId).getValue()+",");
        sb.append(System.currentTimeMillis());

        return sb.toString();
    }

    private List<Pair<String, String>> generateProvince() throws Exception {

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
        List<Pair<String, String>> city2province = new ArrayList<>();
        for (String province:provinces.keySet()) {
            provinces.get(province).forEach(city->{
                city2province.add(new Pair(city, province));
            });
        }
        return city2province;
    }
}
