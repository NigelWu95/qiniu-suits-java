package com.qiniu.service.line;

import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.qiniu.service.interfaces.ILineParser;
import com.qiniu.util.Json;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JsonObjParser implements ILineParser<JsonObject> {

    private Map<String, String> indexMap;

    public JsonObjParser(Map<String, String> indexMap) {
        this.indexMap = indexMap;
    }

    public Map<String, String> getItemMap(JsonObject json) throws IOException {
        Map<String, String> itemMap = new HashMap<>();
        String mapKey;
        for (String key : json.keySet()) {
            mapKey = indexMap.get(key);
            if (mapKey != null) {
                if (!(json.get(key) instanceof JsonNull)) itemMap.put(mapKey, json.get(key).getAsString());
            }
        }
        if (itemMap.size() < indexMap.size()) throw new IOException("no enough indexes in line.");
        return itemMap;
    }
}
