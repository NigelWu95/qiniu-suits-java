package com.qiniu.line;

import com.google.gson.JsonObject;
import com.qiniu.interfaces.IStringFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MapToJsonFormatter implements IStringFormat<Map<String, String>> {

    private List<String> rmFields;

    public MapToJsonFormatter(List<String> removeFields) {
        this.rmFields = removeFields == null ? new ArrayList<>() : removeFields;
    }

    public String toFormatString(Map<String, String> infoMap) {
        JsonObject converted = new JsonObject();
        Set<String> set = infoMap.keySet();
        List<String> keys = new ArrayList<String>(){{
            this.addAll(set);
        }};
        keys.removeAll(rmFields);
        if (keys.contains("key")) {
            converted.addProperty("key", infoMap.get("key"));
            keys.remove("key");
        }
        if (keys.contains("hash")) {
            converted.addProperty("hash", infoMap.get("hash"));
            keys.remove("hash");
        }
        if (keys.contains("fsize")) {
            converted.addProperty("fsize", Long.valueOf(infoMap.get("fsize")));
            keys.remove("fsize");
        }
        if (keys.contains("putTime")) {
            converted.addProperty("putTime", Long.valueOf(infoMap.get("putTime")));
            keys.remove("putTime");
        }
        if (keys.contains("mimeType")) {
            converted.addProperty("mimeType", infoMap.get("mimeType"));
            keys.remove("mimeType");
        }
        if (keys.contains("type")) {
            converted.addProperty("type", Integer.valueOf(infoMap.get("type")));
            keys.remove("type");
        }
        if (keys.contains("status")) {
            converted.addProperty("status", Integer.valueOf(infoMap.get("status")));
            keys.remove("status");
        }
        if (keys.contains("endUser")) {
            converted.addProperty("endUser", infoMap.get("endUser"));
            keys.remove("endUser");
        }
        for (String key : keys) {
            converted.addProperty(key, infoMap.get(key));
        }
        return converted.toString();
    }
}
