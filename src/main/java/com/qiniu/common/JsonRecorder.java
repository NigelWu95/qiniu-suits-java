package com.qiniu.common;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class JsonRecorder {

    private volatile JsonObject prefixesJson = new JsonObject();

    public synchronized void put(String key, JsonObject continueConf) {
        prefixesJson.add(key, continueConf);
    }

    public synchronized void remove(String key) {
        prefixesJson.remove(key);
    }

    public synchronized JsonObject get(String key) {
        JsonElement jsonElement = prefixesJson.get(key);
        if (jsonElement instanceof JsonObject) return jsonElement.getAsJsonObject();
        else return null;
    }

    public JsonObject getOrDefault(String key, JsonObject Default) {
        JsonObject jsonObject = get(key);
        if (jsonObject != null) return jsonObject;
        else return Default;
    }

    public synchronized int size() {
        return prefixesJson.size();
    }

    public synchronized String toString() {
        return prefixesJson.toString();
    }
}
