package com.qiniu.util;

import com.aliyun.oss.model.OSSObjectSummary;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.qcloud.cos.model.COSObjectSummary;
import com.qiniu.storage.model.FileInfo;

import java.io.IOException;
import java.util.*;

public final class LineUtils {

    final static private List<String> longFields = new ArrayList<String>(){{
        add("size");
        add("timestamp");
    }};

    final static private List<String> intFields = new ArrayList<String>(){{
        add("type");
        add("status");
    }};

    final static public List<String> fileInfoFields = new ArrayList<String>(){{
        add("key");
        add("hash");
        addAll(longFields);
        add("datetime");
        add("mime");
        addAll(intFields);
        add("md5");
        add("owner");
    }};

    public static Map<String, String> getItemMap(FileInfo fileInfo, Map<String, String> indexMap) throws IOException {
        if (fileInfo == null || fileInfo.key == null) throw new IOException("empty file or key.");
        Map<String, String> itemMap = new HashMap<>();
        fileInfoFields.forEach(key -> {
            if (indexMap.get(key) != null) {
                switch (key) {
                    case "key": itemMap.put(indexMap.get(key), fileInfo.key); break;
                    case "hash": itemMap.put(indexMap.get(key), fileInfo.hash); break;
                    case "size": itemMap.put(indexMap.get(key), String.valueOf(fileInfo.fsize)); break;
                    case "datetime": itemMap.put(indexMap.get(key), DatetimeUtils.stringOf(fileInfo.putTime,
                            10000000)); break;
                    case "mime": itemMap.put(indexMap.get(key), fileInfo.mimeType); break;
                    case "type": itemMap.put(indexMap.get(key), String.valueOf(fileInfo.type)); break;
                    case "status": itemMap.put(indexMap.get(key), String.valueOf(fileInfo.status)); break;
//                    case "md5": itemMap.put(key, String.valueOf(fileInfo.md5)); break;
                    case "owner": itemMap.put(indexMap.get(key), fileInfo.endUser); break;
                }
            }
        });
        return itemMap;
    }

    public static Map<String, String> getItemMap(COSObjectSummary cosObject, Map<String, String> indexMap)
            throws IOException {
        if (cosObject == null || cosObject.getKey() == null) throw new IOException("empty cosObjectSummary or key.");
        Map<String, String> itemMap = new HashMap<>();
        fileInfoFields.forEach(key -> {
            if (indexMap.get(key) != null) {
                switch (key) {
                    case "key": itemMap.put(indexMap.get(key), cosObject.getKey()); break;
                    case "hash": itemMap.put(indexMap.get(key), cosObject.getETag()); break;
                    case "size": itemMap.put(indexMap.get(key), String.valueOf(cosObject.getSize())); break;
                    case "datetime": itemMap.put(indexMap.get(key), DatetimeUtils.stringOf(cosObject.getLastModified())); break;
//                    case "mime": itemMap.put(indexMap.get(key), cosObject.); break;
                    case "type": itemMap.put(indexMap.get(key), cosObject.getStorageClass()); break;
//                    case "status": itemMap.put(indexMap.get(key), String.valueOf(cosObject.)); break;
                    case "owner": itemMap.put(indexMap.get(key), cosObject.getOwner().getDisplayName()); break;
                }
            }
        });
        return itemMap;
    }

    public static Map<String, String> getItemMap(OSSObjectSummary ossObject, Map<String, String> indexMap)
            throws IOException {
        if (ossObject == null || ossObject.getKey() == null) throw new IOException("empty cosObjectSummary or key.");
        Map<String, String> itemMap = new HashMap<>();
        fileInfoFields.forEach(key -> {
            if (indexMap.get(key) != null) {
                switch (key) {
                    case "key": itemMap.put(indexMap.get(key), ossObject.getKey()); break;
                    case "hash": itemMap.put(indexMap.get(key), ossObject.getETag()); break;
                    case "size": itemMap.put(indexMap.get(key), String.valueOf(ossObject.getSize())); break;
                    case "datetime": itemMap.put(indexMap.get(key), DatetimeUtils.stringOf(ossObject.getLastModified())); break;
                    case "type": itemMap.put(indexMap.get(key), ossObject.getStorageClass()); break;
                    case "owner": itemMap.put(indexMap.get(key), ossObject.getOwner().getDisplayName()); break;
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
        if (rmFields == null || !rmFields.contains("size")) converted.append(fileInfo.fsize).append(separator);
        if (rmFields == null || !rmFields.contains("datetime")) converted.append(
                DatetimeUtils.stringOf(fileInfo.putTime, 10000000)).append(separator);
        if (rmFields == null || !rmFields.contains("mime")) converted.append(fileInfo.mimeType).append(separator);
        if (rmFields == null || !rmFields.contains("type")) converted.append(fileInfo.type).append(separator);
        if (rmFields == null || !rmFields.contains("status")) converted.append(fileInfo.status).append(separator);
//        if (rmFields == null || !rmFields.contains("md5")) converted.append(fileInfo.md5).append(separator);
        if ((rmFields == null || !rmFields.contains("owner")) && fileInfo.endUser != null)
            converted.append(fileInfo.endUser).append(separator);
        if (converted.length() < separator.length()) throw new IOException("empty result.");
        return converted.deleteCharAt(converted.length() - separator.length()).toString();
    }

    public static String toFormatString(FileInfo fileInfo, List<String> rmFields) throws IOException {
        if (fileInfo == null || fileInfo.key == null) throw new IOException("empty file or key.");
        JsonObject converted = new JsonObject();
        if (rmFields == null || !rmFields.contains("key")) converted.addProperty("key", fileInfo.key);
        if (rmFields == null || !rmFields.contains("hash")) converted.addProperty("hash", fileInfo.hash);
        if (rmFields == null || !rmFields.contains("size")) converted.addProperty("size", fileInfo.fsize);
        if (rmFields == null || !rmFields.contains("datetime")) converted.addProperty("datetime",
                DatetimeUtils.stringOf(fileInfo.putTime, 10000000));
        if (rmFields == null || !rmFields.contains("mime")) converted.addProperty("mime", fileInfo.mimeType);
        if (rmFields == null || !rmFields.contains("type")) converted.addProperty("type", fileInfo.type);
        if (rmFields == null || !rmFields.contains("status")) converted.addProperty("status", fileInfo.status);
//        if (rmFields == null || !rmFields.contains("md5")) converted.addProperty("md5", fileInfo.md5);
        if ((rmFields == null || !rmFields.contains("owner")) && fileInfo.endUser != null)
            converted.addProperty("owner", fileInfo.endUser);
        if (converted.size() == 0) throw new IOException("empty result.");
        return converted.toString();
    }

    public static String toFormatString(COSObjectSummary cosObject, List<String> rmFields) throws IOException {
        if (cosObject == null || cosObject.getKey() == null) throw new IOException("empty cosObjectSummary or key.");
        JsonObject converted = new JsonObject();
        if (rmFields == null || !rmFields.contains("key")) converted.addProperty("key", cosObject.getKey());
        if (rmFields == null || !rmFields.contains("hash")) converted.addProperty("hash", cosObject.getETag());
        if (rmFields == null || !rmFields.contains("size")) converted.addProperty("size", cosObject.getSize());
        if (rmFields == null || !rmFields.contains("datetime")) converted.addProperty("datetime",
                DatetimeUtils.stringOf(cosObject.getLastModified()));
        if (rmFields == null || !rmFields.contains("type")) converted.addProperty("type", cosObject.getStorageClass());
        if ((rmFields == null || !rmFields.contains("owner")) && cosObject.getOwner() != null)
            converted.addProperty("owner", cosObject.getOwner().getDisplayName());
        if (converted.size() == 0) throw new IOException("empty result.");
        return converted.toString();
    }

    public static String toFormatString(COSObjectSummary cosObject, String separator, List<String> rmFields)
            throws IOException {
        if (cosObject == null || cosObject.getKey() == null) throw new IOException("empty cosObjectSummary or key.");
        StringBuilder converted = new StringBuilder();
        if (rmFields == null || !rmFields.contains("key")) converted.append(cosObject.getKey()).append(separator);
        if (rmFields == null || !rmFields.contains("hash")) converted.append(cosObject.getETag()).append(separator);
        if (rmFields == null || !rmFields.contains("size")) converted.append(cosObject.getSize()).append(separator);
        if (rmFields == null || !rmFields.contains("datetime")) converted.append(
                DatetimeUtils.stringOf(cosObject.getLastModified())).append(separator);
        if (rmFields == null || !rmFields.contains("type")) converted.append(cosObject.getStorageClass()).append(separator);
        if ((rmFields == null || !rmFields.contains("owner")) && cosObject.getOwner() != null)
            converted.append(cosObject.getOwner().getDisplayName()).append(separator);
        if (converted.length() < separator.length()) throw new IOException("empty result.");
        return converted.deleteCharAt(converted.length() - separator.length()).toString();
    }

    public static String toFormatString(OSSObjectSummary ossObject, List<String> rmFields) throws IOException {
        if (ossObject == null || ossObject.getKey() == null) throw new IOException("empty cosObjectSummary or key.");
        JsonObject converted = new JsonObject();
        if (rmFields == null || !rmFields.contains("key")) converted.addProperty("key", ossObject.getKey());
        if (rmFields == null || !rmFields.contains("hash")) converted.addProperty("hash", ossObject.getETag());
        if (rmFields == null || !rmFields.contains("size")) converted.addProperty("size", ossObject.getSize());
        if (rmFields == null || !rmFields.contains("datetime")) converted.addProperty("datetime",
                DatetimeUtils.stringOf(ossObject.getLastModified()));
        if (rmFields == null || !rmFields.contains("type")) converted.addProperty("type", ossObject.getStorageClass());
        if ((rmFields == null || !rmFields.contains("owner")) && ossObject.getOwner() != null)
            converted.addProperty("owner", ossObject.getOwner().getDisplayName());
        if (converted.size() == 0) throw new IOException("empty result.");
        return converted.toString();
    }

    public static String toFormatString(OSSObjectSummary ossObject, String separator, List<String> rmFields)
            throws IOException {
        if (ossObject == null || ossObject.getKey() == null) throw new IOException("empty cosObjectSummary or key.");
        StringBuilder converted = new StringBuilder();
        if (rmFields == null || !rmFields.contains("key")) converted.append(ossObject.getKey()).append(separator);
        if (rmFields == null || !rmFields.contains("hash")) converted.append(ossObject.getETag()).append(separator);
        if (rmFields == null || !rmFields.contains("size")) converted.append(ossObject.getSize()).append(separator);
        if (rmFields == null || !rmFields.contains("datetime")) converted.append(
                DatetimeUtils.stringOf(ossObject.getLastModified())).append(separator);
        if (rmFields == null || !rmFields.contains("type")) converted.append(ossObject.getStorageClass()).append(separator);
        if ((rmFields == null || !rmFields.contains("owner")) && ossObject.getOwner() != null)
            converted.append(ossObject.getOwner().getDisplayName()).append(separator);
        if (converted.length() < separator.length()) throw new IOException("empty result.");
        return converted.deleteCharAt(converted.length() - separator.length()).toString();
    }

    public static String toFormatString(JsonObject json, String separator, List<String> rmFields) throws IOException {
        if (json == null) throw new IOException("empty JsonObject.");
        StringBuilder converted = new StringBuilder();
        Set<String> set = json.keySet();
        List<String> keys = new ArrayList<String>(){{
            addAll(set);
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
            addAll(set);
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
            addAll(set);
        }};
        if (rmFields != null) keys.removeAll(rmFields);
        fileInfoFields.forEach(key -> {
            if (keys.contains(key) && line.get(key) != null) {
                if (longFields.contains(key)) converted.append(Long.valueOf(line.get(key))).append(separator);
                else if (intFields.contains(key)) converted.append(Integer.valueOf(line.get(key))).append(separator);
                else converted.append(line.get(key)).append(separator);
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
