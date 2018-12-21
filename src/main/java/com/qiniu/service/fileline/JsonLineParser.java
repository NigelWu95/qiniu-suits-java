package com.qiniu.service.fileline;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.qiniu.service.interfaces.ILineParser;

import java.util.*;

public class JsonLineParser implements ILineParser {

    private Map<String, String> infoIndexMap;

    public JsonLineParser(Map<String, String> infoIndexMap) {
        this.infoIndexMap = infoIndexMap;
    }

    public Map<String, String> getItemMap(String line) {
        JsonObject parsed = new JsonParser().parse(line).getAsJsonObject();
        Map<String, String> itemMap = new HashMap<>();
        for (String key : parsed.keySet()) {
            String mapKey = infoIndexMap.get(key);
            if (mapKey != null) itemMap.put(mapKey, parsed.get(key).getAsString());
        }
        return itemMap;
    }
}
