package com.qiniu.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.qiniu.model.ListV2Line;
import com.qiniu.storage.model.FileInfo;

import java.util.List;
import java.util.stream.Collectors;

public class ListUtils {

    public static String marker (int type, String key) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("c", type);
        jsonObject.addProperty("k", key);
        return UrlSafeBase64.encodeToString(JsonConvertUtils.toJson(jsonObject));
    }
}