package com.qiniu.util;

import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.qcloud.cos.model.COSObjectSummary;
import com.qiniu.storage.model.FileInfo;

import java.io.IOException;
import java.util.*;

public class LineUtils {

    final static private List<String> longFields = new ArrayList<String>(){{
        add("fsize");
        add("putTime");
    }};

    final static private List<String> intFields = new ArrayList<String>(){{
        add("type");
        add("status");
    }};

    final static public List<String> fileInfoFields = new ArrayList<String>(){{
        add("key");
        add("hash");
        addAll(longFields);
        add("mimeType");
        addAll(intFields);
        add("md5");
        add("endUser");
    }};

    public static Map<String, String> getItemMap(FileInfo fileInfo, Map<String, String> indexMap) throws IOException {
        if (fileInfo == null || fileInfo.key == null) throw new IOException("empty file or key.");
        Map<String, String> itemMap = new HashMap<>();
        fileInfoFields.forEach(key -> {
            if (indexMap.get(key) != null) {
                switch (key) {
                    case "key": itemMap.put(indexMap.get(key), fileInfo.key); break;
                    case "hash": itemMap.put(indexMap.get(key), fileInfo.hash); break;
                    case "fsize": itemMap.put(indexMap.get(key), String.valueOf(fileInfo.fsize)); break;
                    case "putTime": itemMap.put(indexMap.get(key), String.valueOf(fileInfo.putTime)); break;
                    case "mimeType": itemMap.put(indexMap.get(key), fileInfo.mimeType); break;
                    case "type": itemMap.put(indexMap.get(key), String.valueOf(fileInfo.type)); break;
                    case "status": itemMap.put(indexMap.get(key), String.valueOf(fileInfo.status)); break;
//                    case "md5": itemMap.put(key, String.valueOf(fileInfo.md5)); break;
                    case "endUser": itemMap.put(indexMap.get(key), fileInfo.endUser); break;
                }
            }
        });
        return itemMap;
    }

    public static Map<String, String> getItemMap(COSObjectSummary cosObjectSummary, Map<String, String> indexMap)
            throws IOException {
        if (cosObjectSummary == null || cosObjectSummary.getKey() == null) throw new IOException("empty cosObjectSummary or key.");
        Map<String, String> itemMap = new HashMap<>();
        fileInfoFields.forEach(key -> {
            if (indexMap.get(key) != null) {
                switch (key) {
                    case "key": itemMap.put(indexMap.get(key), cosObjectSummary.getKey()); break;
                    case "hash": itemMap.put(indexMap.get(key), cosObjectSummary.getETag()); break;
                    case "fsize": itemMap.put(indexMap.get(key), String.valueOf(cosObjectSummary.getSize())); break;
                    case "putTime": itemMap.put(indexMap.get(key), String.valueOf(cosObjectSummary.getLastModified())); break;
//                    case "mimeType": itemMap.put(indexMap.get(key), cosObjectSummary.); break;
                    case "type": itemMap.put(indexMap.get(key), String.valueOf(cosObjectSummary.getStorageClass())); break;
//                    case "status": itemMap.put(indexMap.get(key), String.valueOf(cosObjectSummary.)); break;
                    case "endUser": itemMap.put(indexMap.get(key), cosObjectSummary.getOwner().getDisplayName()); break;
                }
            }
        });
        return itemMap;
    }

    public static Map<String, String> getItemMap(JsonObject json, Map<String, String> indexMap, boolean force)
            throws IOException {
        if (json == null) throw new IOException("empty JsonObject.");
        Map<String, String> itemMap = new HashMap<>();
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
        if (line == null) throw new IOException("empty json line.");
        JsonObject parsed = new JsonParser().parse(line).getAsJsonObject();
        return getItemMap(parsed, indexMap, force);
    }

    public static Map<String, String> getItemMap(String line, String separator, Map<String, String> indexMap,
                                                 boolean force) throws IOException {
        if (line == null) throw new IOException("empty string line.");
        String[] items = line.split(separator);
        Map<String, String> itemMap = new HashMap<>();
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
        if (fileInfo == null || fileInfo.key == null) throw new IOException("empty file or key.");
        StringBuilder converted = new StringBuilder();
        if (rmFields == null || !rmFields.contains("key")) converted.append(fileInfo.key).append(separator);
        if (rmFields == null || !rmFields.contains("hash")) converted.append(fileInfo.hash).append(separator);
        if (rmFields == null || !rmFields.contains("fsize")) converted.append(fileInfo.fsize).append(separator);
        if (rmFields == null || !rmFields.contains("putTime")) converted.append(fileInfo.putTime).append(separator);
        if (rmFields == null || !rmFields.contains("mimeType")) converted.append(fileInfo.mimeType).append(separator);
        if (rmFields == null || !rmFields.contains("type")) converted.append(fileInfo.type).append(separator);
        if (rmFields == null || !rmFields.contains("status")) converted.append(fileInfo.status).append(separator);
//        if (rmFields == null || !rmFields.contains("md5")) converted.append(fileInfo.md5).append(separator);
        if ((rmFields == null || !rmFields.contains("endUser")) && fileInfo.endUser != null)
            converted.append(fileInfo.endUser).append(separator);
        if (converted.length() < separator.length()) throw new IOException("empty result.");
        return converted.deleteCharAt(converted.length() - separator.length()).toString();
    }

    public static String toFormatString(FileInfo fileInfo, List<String> rmFields) throws IOException {
        if (fileInfo == null || fileInfo.key == null) throw new IOException("empty file or key.");
        JsonObject converted = new JsonObject();
        if (rmFields == null || !rmFields.contains("key")) converted.addProperty("key", fileInfo.key);
        if (rmFields == null || !rmFields.contains("hash")) converted.addProperty("hash", fileInfo.hash);
        if (rmFields == null || !rmFields.contains("fsize")) converted.addProperty("fsize", fileInfo.fsize);
        if (rmFields == null || !rmFields.contains("putTime")) converted.addProperty("putTime", fileInfo.putTime);
        if (rmFields == null || !rmFields.contains("mimeType")) converted.addProperty("mimeType", fileInfo.mimeType);
        if (rmFields == null || !rmFields.contains("type")) converted.addProperty("type", fileInfo.type);
        if (rmFields == null || !rmFields.contains("status")) converted.addProperty("status", fileInfo.status);
//        if (rmFields == null || !rmFields.contains("md5")) converted.addProperty("md5", fileInfo.md5);
        if ((rmFields == null || !rmFields.contains("endUser")) && fileInfo.endUser != null)
            converted.addProperty("endUser", fileInfo.endUser);
        if (converted.size() == 0) throw new IOException("empty result.");
        return converted.toString();
    }

    public static String toFormatString(COSObjectSummary cosObjectSummary, List<String> rmFields) throws IOException {
        if (cosObjectSummary == null || cosObjectSummary.getKey() == null) throw new IOException("empty cosObjectSummary or key.");
        JsonObject converted = new JsonObject();
        if (rmFields == null || !rmFields.contains("key")) converted.addProperty("key", cosObjectSummary.getKey());
        if (rmFields == null || !rmFields.contains("hash")) converted.addProperty("hash", cosObjectSummary.getETag());
        if (rmFields == null || !rmFields.contains("fsize")) converted.addProperty("fsize", cosObjectSummary.getSize());
        if (rmFields == null || !rmFields.contains("putTime")) converted.addProperty("putTime", cosObjectSummary.getLastModified().getTime());
        if (rmFields == null || !rmFields.contains("type")) converted.addProperty("type", cosObjectSummary.getStorageClass());
        if ((rmFields == null || !rmFields.contains("endUser")) && cosObjectSummary.getOwner() != null)
            converted.addProperty("endUser", cosObjectSummary.getOwner().getDisplayName());
        if (converted.size() == 0) throw new IOException("empty result.");
        return converted.toString();
    }

    public static String toFormatString(COSObjectSummary cosObjectSummary, String separator, List<String> rmFields)
            throws IOException {
        if (cosObjectSummary == null || cosObjectSummary.getKey() == null) throw new IOException("empty cosObjectSummary or key.");
        StringBuilder converted = new StringBuilder();
        if (rmFields == null || !rmFields.contains("key")) converted.append(cosObjectSummary.getKey()).append(separator);
        if (rmFields == null || !rmFields.contains("hash")) converted.append(cosObjectSummary.getETag()).append(separator);
        if (rmFields == null || !rmFields.contains("fsize")) converted.append(cosObjectSummary.getSize()).append(separator);
        if (rmFields == null || !rmFields.contains("putTime")) converted.append(cosObjectSummary.getLastModified().getTime()).append(separator);
        if (rmFields == null || !rmFields.contains("type")) converted.append(cosObjectSummary.getStorageClass()).append(separator);
        if ((rmFields == null || !rmFields.contains("endUser")) && cosObjectSummary.getOwner() != null)
            converted.append(cosObjectSummary.getOwner().getDisplayName()).append(separator);
        if (converted.length() < separator.length()) throw new IOException("empty result.");
        return converted.deleteCharAt(converted.length() - separator.length()).toString();
    }

    public static String toFormatString(JsonObject json, String separator, List<String> rmFields) throws IOException {
        if (json == null) throw new IOException("empty JsonObject.");
        StringBuilder converted = new StringBuilder();
        Set<String> set = json.keySet();
        List<String> keys = new ArrayList<String>(){{
            this.addAll(set);
        }};
        if (rmFields != null) keys.removeAll(rmFields);
        fileInfoFields.forEach(key -> {
            if (keys.contains(key) && !(json.get(key) instanceof JsonNull)) {
                if (longFields.contains(key)) converted.append(json.get(key).getAsLong()).append(separator);
                else if (intFields.contains(key)) converted.append(json.get(key).getAsInt()).append(separator);
                else converted.append(json.get(key).getAsString()).append(separator);
            }
            keys.remove(key);
        });
        for (String key : keys) {
            converted.append(json.get(key).getAsString()).append(separator);
        }
        if (converted.length() < separator.length()) throw new IOException("empty result.");
        return converted.deleteCharAt(converted.length() - separator.length()).toString();
    }

    public static String toFormatString(Map<String, String> line, List<String> rmFields) throws IOException {
        if (line == null) throw new IOException("empty string map.");
        JsonObject converted = new JsonObject();
        Set<String> set = line.keySet();
        List<String> keys = new ArrayList<String>(){{
            this.addAll(set);
        }};
        if (rmFields != null) keys.removeAll(rmFields);
        fileInfoFields.forEach(key -> {
            if (keys.contains(key) && line.get(key) != null) {
                if (longFields.contains(key)) converted.addProperty(key, Long.valueOf(line.get(key)));
                else if (intFields.contains(key)) converted.addProperty(key, Integer.valueOf(line.get(key)));
                else converted.addProperty(key, line.get(key));
            }
            keys.remove(key);
        });
        for (String key : keys) {
            converted.addProperty(key, line.get(key));
        }
        if (converted.size() == 0) throw new IOException("empty result.");
        return converted.toString();
    }

    public static String toFormatString(Map<String, String> line, String separator, List<String> rmFields)
            throws IOException {
        if (line == null) throw new IOException("empty string map.");
        StringBuilder converted = new StringBuilder();
        Set<String> set = line.keySet();
        List<String> keys = new ArrayList<String>(){{
            this.addAll(set);
        }};
        if (rmFields != null) keys.removeAll(rmFields);
        fileInfoFields.forEach(key -> {
            if (keys.contains(key) && line.get(key) != null) {
                converted.append(line.get(key)).append(separator);
            }
            keys.remove(key);
        });
        for (String key : keys) {
            converted.append(line.get(key)).append(separator);
        }
        if (converted.length() < separator.length()) throw new IOException("empty result.");
        return converted.deleteCharAt(converted.length() - separator.length()).toString();
    }
}
