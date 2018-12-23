package com.qiniu.service.convert;

import com.google.gson.JsonObject;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.service.interfaces.ITypeConvert;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class InfoMapToString implements ITypeConvert<Map<String, String>, String> {

    private IStringFormat<Map<String, String>> stringFormatter;
    private List<String> usedFields;
    volatile private List<String> errorList = new ArrayList<>();

    public InfoMapToString(String format, String separator, List<String> usedFields) throws IOException {
        if (usedFields == null || usedFields.size() == 0) throw new IOException("there are no fields be set.");
        this.usedFields = usedFields;
        if ("format".equals(format)) {
            stringFormatter = (infoMap, fields) -> {
                JsonObject converted = new JsonObject();
                fields.forEach(key -> converted.addProperty(key, infoMap.get(key)));
                if (converted.size() == 0) throw new IOException("there are no valid info map key in fields.");
                return converted.getAsString();
            };
        } else {
            stringFormatter = (infoMap, fields) -> {
                StringBuilder converted = new StringBuilder();
                fields.forEach(key -> {
                        converted.append(infoMap.get(key));
                        converted.append(separator);
                });
                if (converted.toString().split(separator).length == 0)
                    throw new IOException("there are no valid info map key in fields.");
                return converted.toString();
            };
        }
    }

    public List<String> convertToVList(List<Map<String, String>> srcList) throws IOException {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        List<String> resultList = srcList.parallelStream()
                .filter(Objects::nonNull)
                .map(infoMap -> {
                    try {
                        return stringFormatter.toFormatString(infoMap, usedFields);
                    } catch (Exception e) {
                        errorList.add(infoMap.toString());
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (errorList.size() == srcList.size()) throw new IOException("covert map by fields failed, " +
                "please check the save fields' setting.");
        return resultList;
    }

    public List<String> getErrorList() {
        return errorList;
    }
}
