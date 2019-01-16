package com.qiniu.service.line;

import com.google.gson.JsonObject;
import com.qiniu.service.interfaces.IStringFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MapToJsonFormatter implements IStringFormat<Map<String, String>> {

    private List<String> rmFields;
    final private List<String> fileInfoFields = new ArrayList<String>(){{
        add("key");
        add("hash");
        add("fsize");
        add("putTime");
        add("mimeType");
        add("type");
        add("status");
        add("endUser");
    }};

    public MapToJsonFormatter(List<String> removeFields) {
        this.rmFields = removeFields == null ? new ArrayList<>() : removeFields;
    }

    public String toFormatString(Map<String, String> infoMap) {
        JsonObject converted = new JsonObject();
        if (!rmFields.contains("key") && infoMap.containsKey("key"))
            converted.addProperty("key", infoMap.get("key"));
        if (!rmFields.contains("hash") && infoMap.containsKey("hash"))
            converted.addProperty("hash", infoMap.get("hash"));
        if (!rmFields.contains("fsize") && infoMap.containsKey("fsize"))
            converted.addProperty("fsize", infoMap.get("fsize"));
        if (!rmFields.contains("putTime") && infoMap.containsKey("putTime"))
            converted.addProperty("putTime", infoMap.get("putTime"));
        if (!rmFields.contains("mimeType") && infoMap.containsKey("mimeType"))
            converted.addProperty("mimeType", infoMap.get("mimeType"));
        if (!rmFields.contains("type") && infoMap.containsKey("type"))
            converted.addProperty("type", infoMap.get("type"));
        if (!rmFields.contains("status") && infoMap.containsKey("status"))
            converted.addProperty("status", infoMap.get("status"));
        if (!rmFields.contains("endUser") && infoMap.containsKey("endUser"))
            converted.addProperty("endUser", infoMap.get("endUser"));
        for (Map.Entry<String, String> set : infoMap.entrySet()) {
            if (!rmFields.contains(set.getKey()) && !fileInfoFields.contains(set.getKey()))
                converted.addProperty(set.getKey(), set.getValue());
        }
        return converted.toString();
    }
}
