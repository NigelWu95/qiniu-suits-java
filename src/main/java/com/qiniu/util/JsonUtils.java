package com.qiniu.util;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

import java.util.List;
import java.util.Map;

public final class JsonUtils {

    private static Gson gson = new Gson();
    private static Gson escapeGson = new GsonBuilder().disableHtmlEscaping().create();
    private static JsonParser jsonParser = new JsonParser();

    public static boolean isNull(JsonElement jsonElement) {
        return jsonElement == null || jsonElement instanceof JsonNull;
    }

    public static JsonObject getOrNew(JsonObject jsonObject, String key) {
        if (jsonObject.has(key) && !isNull(jsonObject.get(key))) {
            return jsonObject.getAsJsonObject(key);
        } else {
            return new JsonObject();
        }
    }

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
        return escapeGson.toJson(srcObject)
                .replace(":\"{\\\"", ":{\"")
                .replace("\\\"}\",", "\"},")
                .replace("\\\":\\\"", "\":\"")
                .replace("\\\\", "\\");
    }

    public static JsonObject toJsonObject(Map<String, String> map) {
        JsonObject jsonObject = new JsonObject();
        for (String key : map.keySet()) {
            jsonObject.addProperty(key, map.get(key));
        }
        return jsonObject;
    }

    public static <T> List<T> fromJsonArray(JsonArray jsonElements, TypeToken<List<T>> typeToken) {
        return gson.fromJson(jsonElements, typeToken.getType());
    }
}
