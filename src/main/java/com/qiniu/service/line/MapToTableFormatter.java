package com.qiniu.service.line;

import com.qiniu.service.interfaces.IStringFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MapToTableFormatter implements IStringFormat<Map<String, String>> {

    private String separator;
    private List<String> rmFields;

    public MapToTableFormatter(String separator, List<String> removeFields) {
        this.separator = separator;
        this.rmFields = removeFields == null ? new ArrayList<>() : removeFields;
    }

    public String toFormatString(Map<String, String> infoMap) {
        StringBuilder converted = new StringBuilder();
        Set<String> set = infoMap.keySet();
        List<String> keys = new ArrayList<String>(){{
            this.addAll(set);
        }};
        keys.removeAll(rmFields);
        if (keys.contains("key")) {
            converted.append(infoMap.get("key")).append(separator);
            keys.remove("key");
        }
        if (keys.contains("hash")) {
            converted.append(infoMap.get("hash")).append(separator);
            keys.remove("hash");
        }
        if (keys.contains("fsize")) {
            converted.append(infoMap.get("fsize")).append(separator);
            keys.remove("fsize");
        }
        if (keys.contains("putTime")) {
            converted.append(infoMap.get("putTime")).append(separator);
            keys.remove("putTime");
        }
        if (keys.contains("mimeType")) {
            converted.append(infoMap.get("mimeType")).append(separator);
            keys.remove("mimeType");
        }
        if (keys.contains("type")) {
            converted.append(infoMap.get("type")).append(separator);
            keys.remove("type");
        }
        if (keys.contains("status")) {
            converted.append(infoMap.get("status")).append(separator);
            keys.remove("status");
        }
        if (keys.contains("endUser")) {
            converted.append(infoMap.get("endUser")).append(separator);
            keys.remove("endUser");
        }
        for (String key : keys) {
            converted.append(infoMap.get(key)).append(separator);
        }
        return converted.deleteCharAt(converted.length() - 1).toString();
    }
}
