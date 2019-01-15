package com.qiniu.service.convert;

import com.google.gson.JsonObject;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.service.interfaces.ITypeConvert;

import java.io.IOException;
import java.util.*;
import java.util.Map.*;
import java.util.stream.Collectors;

public class InfoMapToString implements ITypeConvert<Map<String, String>, String> {

    private IStringFormat<Map<String, String>> stringFormatter;

    public InfoMapToString(String format, String separator, List<String> removeFields) {
        List<String> rmFields = removeFields == null ? new ArrayList<>() : removeFields;
        if ("json".equals(format)) {
            stringFormatter = (infoMap) -> {
                JsonObject converted = new JsonObject();
                if (!rmFields.contains("key") && infoMap.containsKey("key")) {
                    converted.addProperty("key", infoMap.get("key"));
                    infoMap.remove("key");
                }
                if (!rmFields.contains("hash") && infoMap.containsKey("hash")) {
                    converted.addProperty("hash", infoMap.get("hash"));
                    infoMap.remove("hash");
                }
                for (Entry<String, String> set : infoMap.entrySet()) {
                    if (!rmFields.contains(set.getKey())) converted.addProperty(set.getKey(), set.getValue());
                }
                return converted.toString();
            };
        } else {
            stringFormatter = (infoMap) -> {
                StringBuilder converted = new StringBuilder();
                if (!rmFields.contains("key") && infoMap.containsKey("key")) {
                    converted.append(infoMap.get("key")).append(separator);
                    infoMap.remove("key");
                }
                if (!rmFields.contains("hash") && infoMap.containsKey("hash")) {
                    converted.append(infoMap.get("hash")).append(separator);
                    infoMap.remove("hash");
                }
                for (Entry<String, String> set : infoMap.entrySet()) {
                    if (!rmFields.contains(set.getKey())) converted.append(set.getValue()).append(separator);
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
