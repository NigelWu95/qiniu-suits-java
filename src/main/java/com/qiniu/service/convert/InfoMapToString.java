package com.qiniu.service.convert;

import com.google.gson.JsonObject;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.service.interfaces.ITypeConvert;

import java.util.*;
import java.util.stream.Collectors;

public class InfoMapToString implements ITypeConvert<Map<String, String>, String> {

    private IStringFormat<Map<String, String>> stringFormatter;
    private Map<String, Boolean> variablesIfUse;

    public InfoMapToString(String format, String separator, boolean hash, boolean fsize, boolean putTime,
                           boolean mimeType, boolean endUser, boolean type, boolean status) {
        if ("format".equals(format)) {
            stringFormatter = (infoMap, variablesIfUse) -> {
                JsonObject converted = new JsonObject();
                variablesIfUse.forEach((key, value) -> {
                    if (value) {
                        converted.addProperty(key, String.valueOf(infoMap.get(key)));
                    }
                });
                return converted.toString();
            };
        } else {
            stringFormatter = (infoMap, variablesIfUse) -> {
                StringBuilder converted = new StringBuilder();
                variablesIfUse.forEach((key, value) -> {
                    if (value) {
                        converted.append(String.valueOf(infoMap.get(key)));
                        converted.append(separator);
                    }
                });
                return converted.toString();
            };
        }
        variablesIfUse = new HashMap<>();
        variablesIfUse.put("key", true);
        variablesIfUse.put("hash", hash);
        variablesIfUse.put("fsize", fsize);
        variablesIfUse.put("putTime", putTime);
        variablesIfUse.put("mimeType", mimeType);
        variablesIfUse.put("endUser", endUser);
        variablesIfUse.put("type", type);
//        variablesIfUse.put("status", status);
    }

    public List<String> convertToVList(List<Map<String, String>> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        List<Map<String, String>> errorList = new ArrayList<>();
        List<String> successList = srcList.parallelStream()
                .filter(Objects::nonNull)
                .map(infoMap -> {
                    try {
                        return stringFormatter.toFormatString(infoMap, variablesIfUse);
                    } catch (Exception e) {
                        errorList.add(infoMap);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        srcList = errorList;
        return successList;
    }
}
