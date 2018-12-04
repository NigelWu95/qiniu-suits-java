package com.qiniu.service.fileline;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.storage.model.FileInfo;

import java.util.Map;

public class FileInfoJsonFormatter implements IStringFormat<FileInfo> {

    public String toFormatString(FileInfo fileInfo, Map<String, Boolean> variablesIfUse) {

        JsonObject converted = new JsonObject();
        variablesIfUse.forEach((key, value) -> {
            if (value) {
                try {
                    converted.addProperty(key, String.valueOf(fileInfo.getClass().getField(key)));
                } catch (NoSuchFieldException e) {
                    e.printStackTrace();
                }
//                switch (key) {
//                    case "key": converted.addProperty(key, fileInfo.key); break;
//                    case "fsize": converted.addProperty(key, String.valueOf(fileInfo.fsize)); break;
//                    case "putTime": converted.addProperty(key, String.valueOf(fileInfo.putTime)); break;
//                    case "mimeType": converted.addProperty(key, fileInfo.mimeType); break;
//                    case "endUser": converted.addProperty(key, fileInfo.endUser); break;
//                    case "type": converted.addProperty(key, String.valueOf(fileInfo.type)); break;
////                    case "status": jsonObject.addProperty(key, fileInfo.status); break;
//                }
            }
        });
        return converted.toString();
//        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
//        return gson.toJson(converted).replace("\\\\", "\\");
    }
}
