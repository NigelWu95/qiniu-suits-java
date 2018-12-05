package com.qiniu.service.fileline;

import com.google.gson.JsonObject;
import com.qiniu.model.media.Avinfo;
import com.qiniu.service.interfaces.IStringFormat;

import java.util.Map;

public class AvinfoJsonFormatter implements IStringFormat<Avinfo> {

    public String toFormatString(Avinfo avinfo, Map<String, Boolean> variablesIfUse) {

        JsonObject converted = new JsonObject();
        variablesIfUse.forEach((key, value) -> {
            if (value) {
                try {
                    converted.addProperty(key, String.valueOf(avinfo.getClass().getField(key)));
                } catch (NoSuchFieldException e) {
                    e.printStackTrace();
                }
            }
        });
        return converted.toString();
//        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
//        return gson.toJson(converted).replace("\\\\", "\\");
    }
}
