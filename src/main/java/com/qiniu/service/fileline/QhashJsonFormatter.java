package com.qiniu.service.fileline;

import com.google.gson.JsonObject;
import com.qiniu.model.qoss.Qhash;
import com.qiniu.service.interfaces.IStringFormat;

import java.util.Map;

public class QhashJsonFormatter implements IStringFormat<Qhash> {

    public String toFormatString(Qhash qhash, Map<String, Boolean> variablesIfUse) {

        JsonObject converted = new JsonObject();
        variablesIfUse.forEach((key, value) -> {
            if (value) {
                try {
                    converted.addProperty(key, String.valueOf(qhash.getClass().getField(key)));
                } catch (NoSuchFieldException e) {
                    e.printStackTrace();
                }
//                switch (key) {
//                    case "hash": converted.addProperty(key, qhash.hash); break;
//                    case "fsize": converted.addProperty(key, String.valueOf(qhash.fsize)); break;
//                }
            }
        });
        return converted.toString();
//        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
//        return gson.toJson(converted).replace("\\\\", "\\");
    }
}
