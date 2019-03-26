package com.qiniu.util;

import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.qiniu.storage.model.FileInfo;

import java.io.IOException;
import java.util.*;

public class LineUtils {

    final static private List<String> fileInfoFields = new ArrayList<String>(){{
        add("key");
        add("hash");
        add("fsize");
        add("putTime");
        add("mimeType");
        add("type");
        add("status");
        add("md5");
        add("endUser");
    }};

    public static Map<String, String> getItemMap(FileInfo fileInfo) throws IOException {
        if (fileInfo == null || fileInfo.key == null) throw new IOException("empty file info.");
        Map<String, String> itemMap = new HashMap<>();
        fileInfoFields.forEach(key -> {
            switch (key) {
                case "key": itemMap.put(key, fileInfo.key); break;
                case "hash": itemMap.put(key, fileInfo.hash); break;
                case "fsize": itemMap.put(key, String.valueOf(fileInfo.fsize)); break;
                case "putTime": itemMap.put(key, String.valueOf(fileInfo.putTime)); break;
                case "mimeType": itemMap.put(key, fileInfo.mimeType); break;
                case "type": itemMap.put(key, String.valueOf(fileInfo.type)); break;
                case "status": itemMap.put(key, String.valueOf(fileInfo.status)); break;
//                case "md5": itemMap.put(key, String.valueOf(fileInfo.md5)); break;
                case "endUser": itemMap.put(key, fileInfo.endUser); break;
            }
        });
        return itemMap;
    }

    public static Map<String, String> getItemMap(JsonObject json, Map<String, String> indexMap, boolean force)
            throws IOException {
        Map<String, String> itemMap = new HashMap<>();
        if (indexMap == null || indexMap.size() == 0) throw new IOException("no index map to get.");
        String mapKey;
        for (String key : json.keySet()) {
            mapKey = indexMap.get(key);
            if (mapKey != null) {
                if (json.get(key) instanceof JsonNull) itemMap.put(mapKey, null);
                else itemMap.put(mapKey, json.get(key).getAsString());
            }
        }
        // 是否需要强制转换，即使字段数没有达到 indexMap 的要求
        if (!force && itemMap.size() < indexMap.size())
            throw new IOException("no enough indexes in line. The parameter indexes may have incorrect order or name.");
        return itemMap;
    }

    public static Map<String, String> getItemMap(String line, Map<String, String> indexMap, boolean force)
            throws IOException {
        JsonObject parsed = new JsonParser().parse(line).getAsJsonObject();
        return getItemMap(parsed ,indexMap, force);
    }

    public static Map<String, String> getItemMap(String line, String separator, Map<String, String> indexMap,
                                                 boolean force) throws IOException {
        String[] items = line.split(separator);
        Map<String, String> itemMap = new HashMap<>();
        if (indexMap == null || indexMap.size() == 0) throw new IOException("no index map to get.");
        String mapKey;
        for (int i = 0; i < items.length; i++) {
            mapKey = indexMap.get(String.valueOf(i));
            if (mapKey != null) {
                if (items[i] == null) itemMap.put(mapKey, null);
                else itemMap.put(mapKey, items[i]);
            }
        }
        // 是否需要强制转换，即使字段数没有达到 indexMap 的要求
        if (!force && itemMap.size() < indexMap.size())
            throw new IOException("no enough indexes in line. The parameter indexes may have incorrect order or name.");
        return itemMap;
    }

    public static String toFormatString(FileInfo fileInfo, String separator, List<String> rmFields) throws IOException {
        StringBuilder converted = new StringBuilder();
        if (rmFields == null || !rmFields.contains("key")) converted.append(fileInfo.key).append(separator);
        if (rmFields == null || !rmFields.contains("hash")) converted.append(fileInfo.hash).append(separator);
        if (rmFields == null || !rmFields.contains("fsize")) converted.append(fileInfo.fsize).append(separator);
        if (rmFields == null || !rmFields.contains("putTime")) converted.append(fileInfo.putTime).append(separator);
        if (rmFields == null || !rmFields.contains("mimeType")) converted.append(fileInfo.mimeType).append(separator);
        if (rmFields == null || !rmFields.contains("type")) converted.append(fileInfo.type).append(separator);
        if (rmFields == null || !rmFields.contains("status")) converted.append(fileInfo.status).append(separator);
//        if (rmFields == null || !rmFields.contains("md5")) converted.append(fileInfo.md5).append(separator);
        if (rmFields == null || !rmFields.contains("endUser")) converted.append(fileInfo.endUser).append(separator);
        if (converted.length() < separator.length()) throw new IOException("empty result.");
        return converted.deleteCharAt(converted.length() - separator.length()).toString();
    }

    public static String toFormatString(FileInfo fileInfo, List<String> rmFields) throws IOException {
        JsonObject converted = new JsonObject();
        if (rmFields == null || !rmFields.contains("key")) converted.addProperty("key", fileInfo.key);
        if (rmFields == null || !rmFields.contains("hash")) converted.addProperty("hash", fileInfo.hash);
        if (rmFields == null || !rmFields.contains("fsize")) converted.addProperty("fsize", fileInfo.fsize);
        if (rmFields == null || !rmFields.contains("putTime")) converted.addProperty("putTime", fileInfo.putTime);
        if (rmFields == null || !rmFields.contains("mimeType")) converted.addProperty("mimeType", fileInfo.mimeType);
        if (rmFields == null || !rmFields.contains("type")) converted.addProperty("type", fileInfo.type);
        if (rmFields == null || !rmFields.contains("status")) converted.addProperty("status", fileInfo.status);
//        if (rmFields == null || !rmFields.contains("md5")) converted.addProperty("md5", fileInfo.md5);
        if (rmFields == null || !rmFields.contains("endUser")) converted.addProperty("endUser", fileInfo.endUser);
        if (converted.size() == 0) throw new IOException("empty result.");
        return converted.toString();
    }

    public static String toFormatString(JsonObject json, String separator, List<String> rmFields) throws IOException {
        StringBuilder converted = new StringBuilder();
        Set<String> set = json.keySet();
        List<String> keys = new ArrayList<String>(){{
            this.addAll(set);
        }};
        if (rmFields != null) keys.removeAll(rmFields);
        if (keys.contains("key")) {
            converted.append(json.get("key")).append(separator);
            keys.remove("key");
        }
        if (keys.contains("hash")) {
            converted.append(json.get("hash")).append(separator);
            keys.remove("hash");
        }
        if (keys.contains("fsize")) {
            converted.append(json.get("fsize")).append(separator);
            keys.remove("fsize");
        }
        if (keys.contains("putTime")) {
            converted.append(json.get("putTime")).append(separator);
            keys.remove("putTime");
        }
        if (keys.contains("mimeType")) {
            converted.append(json.get("mimeType")).append(separator);
            keys.remove("mimeType");
        }
        if (keys.contains("type")) {
            converted.append(json.get("type")).append(separator);
            keys.remove("type");
        }
        if (keys.contains("status")) {
            converted.append(json.get("status")).append(separator);
            keys.remove("status");
        }
        if (keys.contains("endUser")) {
            converted.append(json.get("endUser")).append(separator);
            keys.remove("endUser");
        }
        for (String key : keys) {
            converted.append(json.get(key)).append(separator);
        }
        if (converted.length() < separator.length()) throw new IOException("empty result.");
        return converted.deleteCharAt(converted.length() - separator.length()).toString();
    }

    public static String toFormatString(Map<String, String> line, List<String> rmFields) throws IOException {
        JsonObject converted = new JsonObject();
        Set<String> set = line.keySet();
        List<String> keys = new ArrayList<String>(){{
            this.addAll(set);
        }};
        if (rmFields != null) keys.removeAll(rmFields);
        if (keys.contains("key")) {
            converted.addProperty("key", line.get("key"));
            keys.remove("key");
        }
        if (keys.contains("hash")) {
            converted.addProperty("hash", line.get("hash"));
            keys.remove("hash");
        }
        if (keys.contains("fsize")) {
            converted.addProperty("fsize", Long.valueOf(line.get("fsize")));
            keys.remove("fsize");
        }
        if (keys.contains("putTime")) {
            converted.addProperty("putTime", Long.valueOf(line.get("putTime")));
            keys.remove("putTime");
        }
        if (keys.contains("mimeType")) {
            converted.addProperty("mimeType", line.get("mimeType"));
            keys.remove("mimeType");
        }
        if (keys.contains("type")) {
            converted.addProperty("type", Integer.valueOf(line.get("type")));
            keys.remove("type");
        }
        if (keys.contains("status")) {
            converted.addProperty("status", Integer.valueOf(line.get("status")));
            keys.remove("status");
        }
        if (keys.contains("endUser")) {
            converted.addProperty("endUser", line.get("endUser"));
            keys.remove("endUser");
        }
        for (String key : keys) {
            converted.addProperty(key, line.get(key));
        }
        if (converted.size() == 0) throw new IOException("empty result.");
        return converted.toString();
    }

    public static String toFormatString(Map<String, String> line, String separator, List<String> rmFields)
            throws IOException {
        StringBuilder converted = new StringBuilder();
        Set<String> set = line.keySet();
        List<String> keys = new ArrayList<String>(){{
            this.addAll(set);
        }};
        if (rmFields != null) keys.removeAll(rmFields);
        if (keys.contains("key")) {
            converted.append(line.get("key")).append(separator);
            keys.remove("key");
        }
        if (keys.contains("hash")) {
            converted.append(line.get("hash")).append(separator);
            keys.remove("hash");
        }
        if (keys.contains("fsize")) {
            converted.append(line.get("fsize")).append(separator);
            keys.remove("fsize");
        }
        if (keys.contains("putTime")) {
            converted.append(line.get("putTime")).append(separator);
            keys.remove("putTime");
        }
        if (keys.contains("mimeType")) {
            converted.append(line.get("mimeType")).append(separator);
            keys.remove("mimeType");
        }
        if (keys.contains("type")) {
            converted.append(line.get("type")).append(separator);
            keys.remove("type");
        }
        if (keys.contains("status")) {
            converted.append(line.get("status")).append(separator);
            keys.remove("status");
        }
        if (keys.contains("endUser")) {
            converted.append(line.get("endUser")).append(separator);
            keys.remove("endUser");
        }
        for (String key : keys) {
            converted.append(line.get(key)).append(separator);
        }
        if (converted.length() < separator.length()) throw new IOException("empty result.");
        return converted.deleteCharAt(converted.length() - separator.length()).toString();
    }
}
