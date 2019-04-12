package com.qiniu.util;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.qiniu.common.Constants;
import com.qiniu.storage.model.FileInfo;

public class OSSUtils {

    public static String calcMarker(FileInfo fileInfo) {
        if (fileInfo == null) return null;
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("c", fileInfo.type);
        jsonObject.addProperty("k", fileInfo.key);
        Gson gson = new Gson();
        String markerJson = gson.toJson(jsonObject);
        return Base64.encodeToString(markerJson.getBytes(Constants.UTF_8), Base64.URL_SAFE | Base64.NO_WRAP);
    }

    public static FileInfo decodeQiniuMarker(String marker) throws JsonParseException {
        if (marker == null) return null;
        String decodedMarker = new String(Base64.decode(marker, Base64.URL_SAFE | Base64.NO_WRAP));
        JsonObject jsonObject = new JsonParser().parse(decodedMarker).getAsJsonObject();
        FileInfo fileInfo = new FileInfo();
        fileInfo.key = jsonObject.get("k").getAsString();
        fileInfo.type = jsonObject.get("c").getAsInt();
        return fileInfo;
    }
}