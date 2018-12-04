package com.qiniu.service.fileline;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.qiniu.service.interfaces.ILineParser;

import java.util.*;

public class JsonLineParser implements ILineParser {

    private JsonObject parseJsonLine(String line) {
        JsonParser jsonParser = new JsonParser();
        return jsonParser.parse(line).getAsJsonObject();
    }

    public ArrayList<String> parseLine(String line) {
        ArrayList<String> result = new ArrayList<>();
        try {
            JsonObject parsed = parseJsonLine(line);
            Set<String> keys = parsed.keySet();
            Gson gson = new Gson();
            for (String key : keys) {
                result.add(gson.fromJson(parsed.get(key), String.class));
            }
            return result;
        } catch (JsonParseException e) {
            return result;
        }
    }

    public Map<String, String> getItemMapByKeys(String line, ArrayList<String> itemKey) {
        List<String> itemList = parseLine(line);
        Map<String, String> itemMap = new HashMap<>();
        for (int i = 0; i < itemKey.size(); i++) {
            itemMap.put(itemKey.get(i), itemList.get(i));
        }

        return itemMap;
    }

    public Map<String, String> getItemMap(String line) {
        Map<String, String> itemMap = new HashMap<>();
        try {
            JsonObject parsed = parseJsonLine(line);
            Gson gson = new Gson();
            return gson.fromJson(parsed, Map.class);
        } catch (JsonParseException e) {
            return itemMap;
        }
    }
}
