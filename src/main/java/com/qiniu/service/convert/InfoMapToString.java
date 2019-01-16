package com.qiniu.service.convert;

import com.google.gson.JsonObject;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.service.interfaces.ITypeConvert;

import java.util.*;
import java.util.Map.*;
import java.util.stream.Collectors;

public class InfoMapToString implements ITypeConvert<Map<String, String>, String> {

    private IStringFormat<Map<String, String>> stringFormatter;
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

    public InfoMapToString(String format, String separator, List<String> removeFields) {
        List<String> rmFields = removeFields == null ? new ArrayList<>() : removeFields;
        // 将 file info 的字段逐一进行获取是为了控制输出字段的顺序
        if ("json".equals(format)) {
            stringFormatter = (infoMap) -> {
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
                for (Entry<String, String> set : infoMap.entrySet()) {
                    if (!rmFields.contains(set.getKey()) && !fileInfoFields.contains(set.getKey()))
                        converted.addProperty(set.getKey(), set.getValue());
                }
                return converted.toString();
            };
        } else {
            stringFormatter = (infoMap) -> {
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
                for (Entry<String, String> set : infoMap.entrySet()) {
                    if (!rmFields.contains(set.getKey()) && !fileInfoFields.contains(set.getKey()))
                        converted.append(set.getValue()).append(separator);
                }
                return converted.toString();
            };
        }
    }

    public List<String> convertToVList(List<Map<String, String>> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        // 使用 parallelStream 时，添加错误行至 errorList 需要同步代码块，stream 时可以直接 errorList.add();
        return srcList.stream()
                .map(stringFormatter::toFormatString)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
