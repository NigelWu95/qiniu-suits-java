package com.qiniu.common;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

public class JsonRecorder {

    private volatile JsonObject prefixesJson = new JsonObject();

    public synchronized String put(String key, JsonObject continueConf) {
        prefixesJson.add(key, continueConf);
        return prefixesJson.toString();
    }

    public synchronized String put(String key, String line) {
        prefixesJson.addProperty(key, line);
        return prefixesJson.toString();
    }

    public synchronized void remove(String key) {
        prefixesJson.remove(key);
    }

    public JsonObject getJson(String key) {
        JsonElement jsonElement = prefixesJson.get(key);
        if (jsonElement instanceof JsonObject) return jsonElement.getAsJsonObject();
        else return null;
    }

    public String getString(String key) {
        JsonElement jsonElement = prefixesJson.get(key);
        if (jsonElement == null || jsonElement instanceof JsonNull) return null;
        else {
            try {
                return jsonElement.getAsString();
            } catch (UnsupportedOperationException u) {
                return jsonElement.toString();
            }
        }
    }

    public JsonObject getOrDefault(String key, JsonObject Default) {
        JsonObject jsonObject = getJson(key);
        if (jsonObject != null) return jsonObject;
        else return Default;
    }

    public String getOrDefault(String key, String Default) {
        String value = getString(key);
        if (value != null) return value;
        else return Default;
    }

    public int size() {
        return prefixesJson.size();
    }

    public synchronized String toString() {
        return prefixesJson.toString();
    }
}
