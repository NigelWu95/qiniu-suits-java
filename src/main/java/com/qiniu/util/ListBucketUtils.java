package com.qiniu.util;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.qiniu.common.Constants;
import com.qiniu.storage.model.FileInfo;

public class ListBucketUtils {

    public static String calcMarker(FileInfo fileInfo) {
        if (fileInfo == null) return null;
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("c", fileInfo.type);
        jsonObject.addProperty("k", fileInfo.key);
        Gson gson = new Gson();
        String markerJson = gson.toJson(jsonObject);
        return Base64.encodeToString(markerJson.getBytes(Constants.UTF_8), Base64.URL_SAFE | Base64.NO_WRAP);
    }
}