package com.qiniu.util;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JSONConvertUtils {

    public static final <T> T fromJson(String jsonData, Class<T> clazz) {
        Gson gson = new Gson();
        return gson.fromJson(jsonData, clazz);
    }

    public static final JsonObject toJson(String jsonData) {
        JsonParser jsonParser = new JsonParser();
        return jsonParser.parse(jsonData).getAsJsonObject();
    }

    public static final String toJson(Object srcObject) {
        Gson gson = new Gson();
        return gson.toJson(srcObject);
    }
}