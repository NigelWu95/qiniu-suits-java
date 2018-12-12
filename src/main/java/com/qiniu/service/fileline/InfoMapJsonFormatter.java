package com.qiniu.service.fileline;

import com.google.gson.JsonObject;
import com.qiniu.service.interfaces.IStringFormat;

import java.util.Map;

public class InfoMapJsonFormatter implements IStringFormat<Map<String, String>> {

    public String toFormatString(Map<String, String> infoMap, Map<String, Boolean> variablesIfUse) {

        JsonObject converted = new JsonObject();
        variablesIfUse.forEach((key, value) -> {
            if (value) {
                converted.addProperty(key, String.valueOf(infoMap.get(key)));
            }
        });
        return converted.toString();
//        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
//        return gson.toJson(converted).replace("\\\\", "\\");
    }
}
