package com.qiniu.service.convert;

import com.google.gson.JsonObject;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.service.interfaces.ITypeConvert;

import java.util.*;
import java.util.stream.Collectors;

public class InfoMapToString implements ITypeConvert<Map<String, String>, String> {

    private IStringFormat<Map<String, String>> stringFormatter;
    private List<String> usedFields;
    volatile private List<String> errorList = new ArrayList<>();

    public InfoMapToString(String format, String separator, List<String> usedFields) {
        if ("format".equals(format)) {
            stringFormatter = (infoMap, fields) -> {
                JsonObject converted = new JsonObject();
                fields.forEach(key -> converted.addProperty(key, String.valueOf(infoMap.get(key))));
                return converted.toString();
            };
        } else {
            stringFormatter = (infoMap, variablesIfUse) -> {
                StringBuilder converted = new StringBuilder();
                variablesIfUse.forEach(key -> {
                        converted.append(String.valueOf(infoMap.get(key)));
                        converted.append(separator);
                });
                return converted.toString();
            };
        }
        this.usedFields = usedFields;
    }

    public List<String> convertToVList(List<Map<String, String>> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        return srcList.parallelStream()
                .filter(Objects::nonNull)
                .map(infoMap -> {
                    try {
                        return stringFormatter.toFormatString(infoMap, usedFields);
                    } catch (Exception e) {
                        errorList.add(String.valueOf(infoMap));
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public List<String> getErrorList() {
        return errorList;
    }
}
