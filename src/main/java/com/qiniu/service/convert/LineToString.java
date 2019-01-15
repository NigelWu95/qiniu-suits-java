package com.qiniu.service.convert;

import com.google.gson.JsonObject;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.service.interfaces.ITypeConvert;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class LineToString implements ITypeConvert<Map<String, String>, String> {

    private IStringFormat<Map<String, String>> stringFormatter;
    private volatile List<String> errorList = new ArrayList<>();

    public LineToString(String format, String separator, List<String> removeFields) {
        List<String> rmFields = removeFields == null ? new ArrayList<>() : removeFields;
        if ("json".equals(format)) {
            stringFormatter = (infoMap) -> {
                JsonObject converted = new JsonObject();
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
                if (!rmFields.contains("fsize") && infoMap.containsKey("fsize")) {
                    converted.append(infoMap.get("fsize")).append(separator);
                    infoMap.remove("fsize");
                }
                if (!rmFields.contains("putTime") && infoMap.containsKey("putTime")) {
                    converted.append(infoMap.get("putTime")).append(separator);
                    infoMap.remove("putTime");
                }
                if (!rmFields.contains("mimeType") && infoMap.containsKey("mimeType")) {
                    converted.append(infoMap.get("mimeType")).append(separator);
                    infoMap.remove("mimeType");
                }
                if (!rmFields.contains("type") && infoMap.containsKey("type")) {
                    converted.append(infoMap.get("type")).append(separator);
                    infoMap.remove("type");
                }
                if (!rmFields.contains("status") && infoMap.containsKey("status")) {
                    converted.append(infoMap.get("status")).append(separator);
                    infoMap.remove("status");
                }
                if (!rmFields.contains("endUser") && infoMap.containsKey("endUser")) {
                    converted.append(infoMap.get("endUser"));
                    infoMap.remove("endUser");
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
                .map(info -> {
                    try {
                        return stringFormatter.toFormatString(info);
                    } catch (Exception e) {
                        addError(String.valueOf(info) + "\t" + e.getMessage());
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    synchronized private void addError(String errorLine) {
        errorList.add(errorLine);
    }

    public List<String> getErrorList() {
        return errorList;
    }

    public List<String> consumeErrorList() {
        List<String> errors = new ArrayList<>();
        Collections.addAll(errors, new String[errorList.size()]);
        Collections.copy(errors, errorList);
        errorList.clear();
        return errors;
    }
}
