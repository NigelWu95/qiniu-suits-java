package com.qiniu.util;

import com.google.gson.JsonObject;

public class ListUtils {

    public static String marker (int type, String key) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("c", type);
        jsonObject.addProperty("k", key);
        return UrlSafeBase64.encodeToString(JsonConvertUtils.toJson(jsonObject));
    }
}