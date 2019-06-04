package com.qiniu.util;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

import java.util.List;

public final class JsonUtils {

    private static Gson gson = new Gson();
    private static Gson escapeGson = new GsonBuilder().disableHtmlEscaping().create();
    private static JsonParser jsonParser = new JsonParser();

    public static <T> T fromJson(String jsonData, Class<T> clazz) {
        return gson.fromJson(jsonData, clazz);
    }

    public static <T> T fromJson(JsonElement jsonElement, Class<T> clazz) {
        return gson.fromJson(jsonElement, clazz);
    }

    public static JsonObject toJsonObject(String jsonData) {
        return jsonParser.parse(jsonData).getAsJsonObject();
    }

    public static String toJson(String jsonData) {
        return jsonParser.parse(jsonData).toString();
    }

    public static String toJson(Object srcObject) {
        return gson.toJson(srcObject);
    }

    public static String toString(JsonElement jsonElement) {
        return gson.fromJson(jsonElement, String.class);
    }

    public static String toJsonWithoutUrlEscape(Object srcObject) {
        return escapeGson.toJson(srcObject).replace("\\\\", "\\");
    }

    public static <T> List<T> fromJsonArray(JsonArray jsonElements, TypeToken<List<T>> typeToken) {
        return gson.fromJson(jsonElements, typeToken.getType());
    }
}
