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
        set.removeAll(rmFields);
        if (set.contains("key")) {
            converted.append(infoMap.get("key")).append(separator);
            set.remove("key");
        }
        if (set.contains("hash")) {
            converted.append(infoMap.get("hash")).append(separator);
            set.remove("hash");
        }
        if (set.contains("fsize")) {
            converted.append(infoMap.get("fsize")).append(separator);
            set.remove("fsize");
        }
        if (set.contains("putTime")) {
            converted.append(infoMap.get("putTime")).append(separator);
            set.remove("putTime");
        }
        if (set.contains("mimeType")) {
            converted.append(infoMap.get("mimeType")).append(separator);
            set.remove("mimeType");
        }
        if (set.contains("type")) {
            converted.append(infoMap.get("type")).append(separator);
            set.remove("type");
        }
        if (set.contains("status")) {
            converted.append(infoMap.get("status")).append(separator);
            set.remove("status");
        }
        if (set.contains("endUser")) {
            converted.append(infoMap.get("endUser"));
            set.remove("endUser");
        }
        for (String key : set) {
            converted.append(separator).append(infoMap.get(key));
        }
        return converted.toString();
    }
}
