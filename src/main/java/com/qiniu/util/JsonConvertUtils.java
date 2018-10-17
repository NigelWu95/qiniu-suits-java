package com.qiniu.util;

import com.google.gson.*;

public final class JsonConvertUtils {

    public static final <T> T fromJson(String jsonData, Class<T> clazz) {
        Gson gson = new Gson();
        return gson.fromJson(jsonData, clazz);
    }

    public static final <T> T fromJson(JsonElement jsonElement, Class<T> clazz) {
        Gson gson = new Gson();
        return gson.fromJson(jsonElement, clazz);
    }

    public static final JsonObject toJsonObject(String jsonData) {
        JsonParser jsonParser = new JsonParser();
        return jsonParser.parse(jsonData).getAsJsonObject();
    }

    public static final String toJson(Object srcObject) {
        Gson gson = new Gson();
        return gson.toJson(srcObject);
    }
}