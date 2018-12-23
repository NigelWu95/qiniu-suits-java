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

    public InfoMapToString(String format, String separator, List<String> usedFields) throws IOException {
        if (usedFields == null || usedFields.size() == 0) throw new IOException("there are no fields be set.");
        this.usedFields = usedFields;
        if ("format".equals(format)) {
            stringFormatter = (infoMap, fields) -> {
                JsonObject converted = new JsonObject();
                fields.forEach(key -> converted.addProperty(key, infoMap.get(key)));
                return converted.getAsString();
            };
        } else {
            stringFormatter = (infoMap, fields) -> {
                StringBuilder converted = new StringBuilder();
                fields.forEach(key -> {
                        converted.append(infoMap.get(key));
                        converted.append(separator);
                });
                return converted.toString();
            };
        }
    }

    public List<String> convertToVList(List<Map<String, String>> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        return srcList.parallelStream()
                .filter(Objects::nonNull)
                .map(infoMap -> stringFormatter.toFormatString(infoMap, usedFields))
                .collect(Collectors.toList());
    }
}
