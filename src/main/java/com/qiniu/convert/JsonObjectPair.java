package com.qiniu.convert;

import com.google.gson.JsonObject;
import com.qiniu.interfaces.KeyValuePair;

public class JsonObjectPair implements KeyValuePair<String, JsonObject> {

    private JsonObject jsonObject = new JsonObject();

    @Override
    public void putKey(String key, String value) {
        jsonObject.addProperty(key, value);
    }

    @Override
    public void put(String key, String value) {
        jsonObject.addProperty(key, value);
    }

    @Override
    public void put(String key, Boolean value) {
        jsonObject.addProperty(key, value);
    }

    @Override
    public void put(String key, Integer value) {
        jsonObject.addProperty(key, value);
    }

    @Override
    public void put(String key, Long value) {
        jsonObject.addProperty(key, value);
    }

    @Override
    public JsonObject getProtoEntity() {
        return jsonObject;
    }

    @Override
    public int size() {
        return jsonObject.size();
    }
}
