package com.qiniu.service.convert;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.service.interfaces.ITypeConvert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InfoMapToString implements ITypeConvert<Map<String, String>, String> {

    private IStringFormat<Map<String, String>> stringFormatter;
    private Map<String, Boolean> variablesIfUse;

    public InfoMapToString(String format, String separator) {
        if ("format".equals(format)) {
            stringFormatter = (infoMap, variablesIfUse) -> {
                Gson gson = new GsonBuilder().disableHtmlEscaping().create();
                return gson.toJson(infoMap).replace("\\\\", "\\");
            };
        } else {
            stringFormatter = (stringStringMap, variablesIfUse) -> null;
        }
        variablesIfUse = new HashMap<>();
        variablesIfUse.put("key", true);
    }
    public String toV(Map<String, String> stringStringMap) {
        return null;
    }

    public List<String> convertToVList(List<Map<String, String>> srcList) {
        return null;
    }
}
