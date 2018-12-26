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
    volatile private List<String> errorList = new ArrayList<>();

    public InfoMapToString(String format, String separator, List<String> rmFields) {
        List<String> rFields = rmFields == null ? new ArrayList<>() : rmFields;
        if ("format".equals(format)) {
            stringFormatter = (infoMap) -> {
                JsonObject converted = new JsonObject();
                for (Entry<String, String> set : infoMap.entrySet()) {
                    if (!rFields.contains(set.getKey())) converted.addProperty(set.getKey(), set.getValue());
                }
                if (converted.size() < infoMap.size() - rFields.size())
                    throw new IOException("there are no enough valid info map key in fields.");
                return converted.getAsString();
            };
        } else {
            stringFormatter = (infoMap) -> {
                StringBuilder converted = new StringBuilder();
                for (Entry<String, String> set : infoMap.entrySet()) {
                    if (!rFields.contains(set.getKey())) converted.append(set.getValue()).append(separator);
                }
                if (converted.toString().split(separator).length < infoMap.size() - rFields.size())
                    throw new IOException("there are no enough valid info map key in fields.");
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
                        return stringFormatter.toFormatString(infoMap);
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
