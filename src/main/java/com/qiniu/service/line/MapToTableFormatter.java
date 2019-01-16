package com.qiniu.service.line;

import com.qiniu.service.interfaces.IStringFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MapToTableFormatter implements IStringFormat<Map<String, String>> {

    private String separator;
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

    public MapToTableFormatter(String separator, List<String> removeFields) {
        this.separator = separator;
        this.rmFields = removeFields == null ? new ArrayList<>() : removeFields;
    }

    public String toFormatString(Map<String, String> infoMap) {
        StringBuilder converted = new StringBuilder();
        if (!rmFields.contains("key") && infoMap.containsKey("key"))
            converted.append(infoMap.get("key")).append(separator);
        if (!rmFields.contains("hash") && infoMap.containsKey("hash"))
            converted.append(infoMap.get("hash")).append(separator);
        if (!rmFields.contains("fsize") && infoMap.containsKey("fsize"))
            converted.append(infoMap.get("fsize")).append(separator);
        if (!rmFields.contains("putTime") && infoMap.containsKey("putTime"))
            converted.append(infoMap.get("putTime")).append(separator);
        if (!rmFields.contains("mimeType") && infoMap.containsKey("mimeType"))
            converted.append(infoMap.get("mimeType")).append(separator);
        if (!rmFields.contains("type") && infoMap.containsKey("type"))
            converted.append(infoMap.get("type")).append(separator);
        if (!rmFields.contains("status") && infoMap.containsKey("status"))
            converted.append(infoMap.get("status")).append(separator);
        if (!rmFields.contains("endUser") && infoMap.containsKey("endUser"))
            converted.append(infoMap.get("endUser"));
        for (Map.Entry<String, String> set : infoMap.entrySet()) {
            if (!rmFields.contains(set.getKey()) && !fileInfoFields.contains(set.getKey()))
                converted.append(set.getValue()).append(separator);
        }
        return converted.toString();
    }
}
