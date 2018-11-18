package com.qiniu.model;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.qiniu.common.QiniuException;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Json;
import com.qiniu.util.JsonConvertUtils;
import com.qiniu.util.StringUtils;
import com.qiniu.util.UrlSafeBase64;

public class ListV2Line implements Comparable {

    public FileInfo fileInfo;

    public String marker = "";

    public String dir = "";

    public int compareTo(Object object) {
        ListV2Line listV2Line = (ListV2Line) object;
        if (listV2Line.fileInfo == null && this.fileInfo == null) {
            return 0;
        } else if (this.fileInfo == null) {
            if (!"".equals(marker)) {
                String markerJson = new String(UrlSafeBase64.decode(marker));
                String key = JsonConvertUtils.fromJson(markerJson, JsonObject.class).get("k").getAsString();
                return key.compareTo(listV2Line.fileInfo.key);
            }
            return 1;
        } else if (listV2Line.fileInfo == null) {
            if (!"".equals(listV2Line.marker)) {
                String markerJson = new String(UrlSafeBase64.decode(listV2Line.marker));
                String key = JsonConvertUtils.fromJson(markerJson, JsonObject.class).get("k").getAsString();
                return this.fileInfo.key.compareTo(key);
            }
            return -1;
        } else {
            return this.fileInfo.key.compareTo(listV2Line.fileInfo.key);
        }
    }

    public boolean isDeleted() {
        return (fileInfo == null && StringUtils.isNullOrEmpty(dir));
    }

    public ListV2Line fromLine(String line) {

        if (!StringUtils.isNullOrEmpty(line)) {
            JsonObject json = new JsonObject();
            // to test the exceptional line.
            try {
                json = JsonConvertUtils.toJsonObject(line);
            } catch (JsonParseException e) {
                System.out.println(line);
                e.printStackTrace();
            }
            JsonElement item = json.get("item");
            JsonElement marker = json.get("marker");
            if (item != null && !(item instanceof JsonNull)) {
                this.fileInfo = JsonConvertUtils.fromJson(item, FileInfo.class);
            }
            if (marker != null && !(marker instanceof JsonNull)) {
                this.marker = marker.getAsString();
            }
        }
        return this;
    }
}
