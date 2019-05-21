package com.qiniu.util;

import com.aliyun.oss.model.OSSObjectSummary;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.qcloud.cos.model.COSObjectSummary;
import com.qiniu.storage.model.FileInfo;

import java.io.IOException;
import java.util.*;

public final class LineUtils {

    final static private Set<String> hashFields = new HashSet<String>(){{
        add("hash");
        add("etag");
    }};

    final static private Set<String> timeFields = new HashSet<String>(){{
        add("datetime");
        add("timestamp");
        add("putTime");
    }};

    final static private Set<String> sizeFields = new HashSet<String>(){{
        add("size");
        add("fsize");
    }};

    final static private Set<String> mimeFields = new HashSet<String>(){{
        add("mime");
        add("mimeType");
    }};

    final static private Set<String> ownerFields = new HashSet<String>(){{
        add("owner");
        add("endUser");
    }};

    final static private Set<String> intFields = new HashSet<String>(){{
        add("status");
    }};

    final static private Set<String> longFields = new HashSet<String>(){{
        add("timestamp");
        add("putTime");
        addAll(sizeFields);
    }};

    final static public Set<String> fileInfoFields = new HashSet<String>(){{
        add("key");
        add("type");
        add("md5");
        addAll(intFields);
        addAll(longFields);
        addAll(hashFields);
        addAll(timeFields);
        addAll(mimeFields);
        addAll(ownerFields);
    }};

    public static Map<String, String> getItemMap(FileInfo fileInfo, Map<String, String> indexMap) throws IOException {
        if (fileInfo == null || fileInfo.key == null) throw new IOException("empty file or key.");
        Map<String, String> itemMap = new HashMap<>();
        for (String index : indexMap.keySet()) {
            if (!fileInfoFields.contains(index)) {
                throw new IOException("the index: " + index + " can't be found.");
            }
            switch (index) {
                case "key": itemMap.put(indexMap.get(index), fileInfo.key); break;
                case "hash":
                case "etag":
                    itemMap.put(indexMap.get(index), fileInfo.hash); break;
                case "size":
                case "fsize":
                    itemMap.put(indexMap.get(index), String.valueOf(fileInfo.fsize)); break;
                case "datetime":
                    itemMap.put(indexMap.get(index), DatetimeUtils.stringOf(fileInfo.putTime, 10000000)); break;
                case "timestamp":
                case "putTime":
                    itemMap.put(indexMap.get(index), String.valueOf(fileInfo.putTime)); break;
                case "mime":
                case "mimeType":
                    itemMap.put(indexMap.get(index), fileInfo.mimeType); break;
                case "type": itemMap.put(indexMap.get(index), String.valueOf(fileInfo.type)); break;
                case "status": itemMap.put(indexMap.get(index), String.valueOf(fileInfo.status)); break;
                case "owner":
                case "endUser":
                    itemMap.put(indexMap.get(index), fileInfo.endUser); break;
            }
        }
        return itemMap;
    }

    public static Map<String, String> getItemMap(COSObjectSummary cosObject, Map<String, String> indexMap)
            throws IOException {
        if (cosObject == null || cosObject.getKey() == null) throw new IOException("empty cosObjectSummary or key.");
        Map<String, String> itemMap = new HashMap<>();
        for (String index : indexMap.keySet()) {
            if (!fileInfoFields.contains(index)) {
                throw new IOException("the index: " + index + " can't be found.");
            }
            switch (index) {
                case "key": itemMap.put(indexMap.get(index), cosObject.getKey()); break;
                case "hash":
                case "etag":
                    itemMap.put(indexMap.get(index), cosObject.getETag()); break;
                case "size":
                case "fsize":
                    itemMap.put(indexMap.get(index), String.valueOf(cosObject.getSize())); break;
                case "datetime":
                    itemMap.put(indexMap.get(index), DatetimeUtils.stringOf(cosObject.getLastModified())); break;
                case "timestamp":
                case "putTime":
                    itemMap.put(indexMap.get(index), String.valueOf(cosObject.getLastModified().getTime())); break;
                case "type": itemMap.put(indexMap.get(index), cosObject.getStorageClass()); break;
                case "owner":
                case "endUser":
                    itemMap.put(indexMap.get(index), cosObject.getOwner().getDisplayName()); break;
            }
        }
        return itemMap;
    }

    public static Map<String, String> getItemMap(OSSObjectSummary ossObject, Map<String, String> indexMap)
            throws IOException {
        if (ossObject == null || ossObject.getKey() == null) throw new IOException("empty cosObjectSummary or key.");
        Map<String, String> itemMap = new HashMap<>();
        for (String index : indexMap.keySet()) {
            if (!fileInfoFields.contains(index)) {
                throw new IOException("the index: " + index + " can't be found.");
            }
            switch (index) {
                case "key": itemMap.put(indexMap.get(index), ossObject.getKey()); break;
                case "hash":
                case "etag":
                    itemMap.put(indexMap.get(index), ossObject.getETag()); break;
                case "size":
                case "fsize":
                    itemMap.put(indexMap.get(index), String.valueOf(ossObject.getSize())); break;
                case "datetime":
                    itemMap.put(indexMap.get(index), DatetimeUtils.stringOf(ossObject.getLastModified())); break;
                case "timestamp":
                case "putTime":
                    itemMap.put(indexMap.get(index), String.valueOf(ossObject.getLastModified().getTime())); break;
                case "type": itemMap.put(indexMap.get(index), ossObject.getStorageClass()); break;
                case "owner":
                case "endUser":
                    itemMap.put(indexMap.get(index), ossObject.getOwner().getDisplayName()); break;
            }
        }
        return itemMap;
    }

    public static Map<String, String> getItemMap(JsonObject json, Map<String, String> indexMap) throws IOException {
        if (json == null) throw new IOException("empty JsonObject.");
        Map<String, String> itemMap = new HashMap<>();
        for (String index : indexMap.keySet()) {
            if (json.has(index)) itemMap.put(indexMap.get(index), JsonUtils.toString(json.get(index)));
            else throw new IOException("the index: " + index + " can't be found.");
        }
        return itemMap;
    }

    public static Map<String, String> getItemMap(String line, Map<String, String> indexMap) throws IOException {
        if (line == null) throw new IOException("empty json line.");
        JsonObject parsed = new JsonParser().parse(line).getAsJsonObject();
        return getItemMap(parsed, indexMap);
    }

    public static Map<String, String> getItemMap(String line, String separator, Map<String, String> indexMap) throws IOException {
        if (line == null) throw new IOException("empty string line.");
        String[] items = line.split(separator);
        Map<String, String> itemMap = new HashMap<>();
        int position;
        for (String index : indexMap.keySet()) {
            position = Integer.valueOf(index);
            if (items.length > position) itemMap.put(indexMap.get(index), items[position]);
            else throw new IOException("the index: " + index + " can't be found.");
        }
        return itemMap;
    }

    public static String toFormatString(FileInfo fileInfo, Set<String> rmFields) throws IOException {
        if (fileInfo == null || fileInfo.key == null) throw new IOException("empty file or key.");
        JsonObject converted = new JsonObject();
        if (rmFields == null || !rmFields.contains("key")) converted.addProperty("key", fileInfo.key);
        if (rmFields == null || hashFields.stream().noneMatch(rmFields::contains))
            converted.addProperty("hash", fileInfo.hash);
        if (rmFields == null || sizeFields.stream().noneMatch(rmFields::contains))
            converted.addProperty("size", fileInfo.fsize);
        if (rmFields == null || timeFields.stream().noneMatch(rmFields::contains))
            converted.addProperty("datetime", DatetimeUtils.stringOf(fileInfo.putTime, 10000000));
        if (rmFields == null || mimeFields.stream().noneMatch(rmFields::contains))
            converted.addProperty("mime", fileInfo.mimeType);
        if (rmFields == null || !rmFields.contains("type")) converted.addProperty("type", fileInfo.type);
        if (rmFields == null || !rmFields.contains("status")) converted.addProperty("status", fileInfo.status);
//        if (rmFields == null || !rmFields.contains("md5")) converted.addProperty("md5", fileInfo.md5);
        if ((rmFields == null || ownerFields.stream().noneMatch(rmFields::contains)) && fileInfo.endUser != null)
            converted.addProperty("owner", fileInfo.endUser);
        if (converted.size() == 0) throw new IOException("empty result.");
        return converted.toString();
    }

    public static String toFormatString(FileInfo fileInfo, String separator, Set<String> rmFields) throws IOException {
        if (fileInfo == null || fileInfo.key == null) throw new IOException("empty file or key.");
        StringBuilder converted = new StringBuilder();
        if (rmFields == null || !rmFields.contains("key")) converted.append(fileInfo.key).append(separator);
        if (rmFields == null || hashFields.stream().noneMatch(rmFields::contains))
            converted.append(fileInfo.hash).append(separator);
        if (rmFields == null || sizeFields.stream().noneMatch(rmFields::contains))
            converted.append(fileInfo.fsize).append(separator);
        if (rmFields == null || timeFields.stream().noneMatch(rmFields::contains))
            converted.append(DatetimeUtils.stringOf(fileInfo.putTime, 10000000)).append(separator);
        if (rmFields == null || mimeFields.stream().noneMatch(rmFields::contains))
            converted.append(fileInfo.mimeType).append(separator);
        if (rmFields == null || !rmFields.contains("type")) converted.append(fileInfo.type).append(separator);
        if (rmFields == null || !rmFields.contains("status")) converted.append(fileInfo.status).append(separator);
//        if (rmFields == null || !rmFields.contains("md5")) converted.append(fileInfo.md5).append(separator);
        if ((rmFields == null || ownerFields.stream().noneMatch(rmFields::contains)) && fileInfo.endUser != null)
            converted.append(fileInfo.endUser).append(separator);
        if (converted.length() <= separator.length()) throw new IOException("empty result.");
        return converted.deleteCharAt(converted.length() - separator.length()).toString();
    }

    public static String toFormatString(COSObjectSummary cosObject, Set<String> rmFields) throws IOException {
        if (cosObject == null || cosObject.getKey() == null) throw new IOException("empty cosObjectSummary or key.");
        JsonObject converted = new JsonObject();
        if (rmFields == null || !rmFields.contains("key")) converted.addProperty("key", cosObject.getKey());
        if (rmFields == null || hashFields.stream().noneMatch(rmFields::contains))
            converted.addProperty("hash", cosObject.getETag());
        if (rmFields == null || sizeFields.stream().noneMatch(rmFields::contains))
            converted.addProperty("size", cosObject.getSize());
        if (rmFields == null || timeFields.stream().noneMatch(rmFields::contains))
            converted.addProperty("datetime", DatetimeUtils.stringOf(cosObject.getLastModified()));
        if (rmFields == null || !rmFields.contains("type")) converted.addProperty("type", cosObject.getStorageClass());
        if ((rmFields == null || ownerFields.stream().noneMatch(rmFields::contains)) && cosObject.getOwner() != null)
            converted.addProperty("owner", cosObject.getOwner().getDisplayName());
        if (converted.size() == 0) throw new IOException("empty result.");
        return converted.toString();
    }

    public static String toFormatString(COSObjectSummary cosObject, String separator, Set<String> rmFields)
            throws IOException {
        if (cosObject == null || cosObject.getKey() == null) throw new IOException("empty cosObjectSummary or key.");
        StringBuilder converted = new StringBuilder();
        if (rmFields == null || !rmFields.contains("key")) converted.append(cosObject.getKey()).append(separator);
        if (rmFields == null || hashFields.stream().noneMatch(rmFields::contains))
            converted.append(cosObject.getETag()).append(separator);
        if (rmFields == null || sizeFields.stream().noneMatch(rmFields::contains))
            converted.append(cosObject.getSize()).append(separator);
        if (rmFields == null || timeFields.stream().noneMatch(rmFields::contains))
            converted.append(DatetimeUtils.stringOf(cosObject.getLastModified())).append(separator);
        if (rmFields == null || !rmFields.contains("type")) converted.append(cosObject.getStorageClass()).append(separator);
        if ((rmFields == null || ownerFields.stream().noneMatch(rmFields::contains)) && cosObject.getOwner() != null)
            converted.append(cosObject.getOwner().getDisplayName()).append(separator);
        if (converted.length() <= separator.length()) throw new IOException("empty result.");
        return converted.deleteCharAt(converted.length() - separator.length()).toString();
    }

    public static String toFormatString(OSSObjectSummary ossObject, Set<String> rmFields) throws IOException {
        if (ossObject == null || ossObject.getKey() == null) throw new IOException("empty cosObjectSummary or key.");
        JsonObject converted = new JsonObject();
        if (rmFields == null || !rmFields.contains("key")) converted.addProperty("key", ossObject.getKey());
        if (rmFields == null || hashFields.stream().noneMatch(rmFields::contains))
            converted.addProperty("hash", ossObject.getETag());
        if (rmFields == null || sizeFields.stream().noneMatch(rmFields::contains))
            converted.addProperty("size", ossObject.getSize());
        if (rmFields == null || timeFields.stream().noneMatch(rmFields::contains))
            converted.addProperty("datetime", DatetimeUtils.stringOf(ossObject.getLastModified()));
        if (rmFields == null || !rmFields.contains("type")) converted.addProperty("type", ossObject.getStorageClass());
        if ((rmFields == null || ownerFields.stream().noneMatch(rmFields::contains)) && ossObject.getOwner() != null)
            converted.addProperty("owner", ossObject.getOwner().getDisplayName());
        if (converted.size() == 0) throw new IOException("empty result.");
        return converted.toString();
    }

    public static String toFormatString(OSSObjectSummary ossObject, String separator, Set<String> rmFields)
            throws IOException {
        if (ossObject == null || ossObject.getKey() == null) throw new IOException("empty cosObjectSummary or key.");
        StringBuilder converted = new StringBuilder();
        if (rmFields == null || !rmFields.contains("key")) converted.append(ossObject.getKey()).append(separator);
        if (rmFields == null || hashFields.stream().noneMatch(rmFields::contains))
            converted.append(ossObject.getETag()).append(separator);
        if (rmFields == null || sizeFields.stream().noneMatch(rmFields::contains))
            converted.append(ossObject.getSize()).append(separator);
        if (rmFields == null || timeFields.stream().noneMatch(rmFields::contains))
            converted.append(DatetimeUtils.stringOf(ossObject.getLastModified())).append(separator);
        if (rmFields == null || !rmFields.contains("type")) converted.append(ossObject.getStorageClass()).append(separator);
        if ((rmFields == null || ownerFields.stream().noneMatch(rmFields::contains)) && ossObject.getOwner() != null)
            converted.append(ossObject.getOwner().getDisplayName()).append(separator);
        if (converted.length() <= separator.length()) throw new IOException("empty result.");
        return converted.deleteCharAt(converted.length() - separator.length()).toString();
    }

    public static String toFormatString(Map<String, String> line, Set<String> rmFields) throws IOException {
        if (line == null) throw new IOException("empty string map.");
        JsonObject converted = new JsonObject();
        Set<String> set = line.keySet();
        List<String> keys = new ArrayList<String>(){{
            addAll(set);
        }};
        if (rmFields != null) keys.removeAll(rmFields);
        for (String key : keys) {
            if (fileInfoFields.contains(key)) {
                if (longFields.contains(key)) converted.addProperty(key, Long.valueOf(line.get(key)));
                else if (intFields.contains(key)) converted.addProperty(key, Integer.valueOf(line.get(key)));
                else converted.addProperty(key, line.get(key));
            } else {
                converted.addProperty(key, line.get(key));
            }
        }
        if (converted.size() == 0) throw new IOException("empty result.");
        return converted.toString();
    }

    public static String toFormatString(Map<String, String> line, String separator, Set<String> rmFields) throws IOException {
        if (line == null) throw new IOException("empty string map.");
        StringBuilder converted = new StringBuilder();
        Set<String> set = line.keySet();
        List<String> keys = new ArrayList<String>(){{
            addAll(set);
        }};
        if (rmFields != null) keys.removeAll(rmFields);
        for (String key : keys) {
            if (fileInfoFields.contains(key)) {
                if (longFields.contains(key)) converted.append(Long.valueOf(line.get(key))).append(separator);
                else if (intFields.contains(key)) converted.append(Integer.valueOf(line.get(key))).append(separator);
                else converted.append(line.get(key)).append(separator);
            } else {
                converted.append(line.get(key)).append(separator);
            }
        }
        if (converted.length() <= separator.length()) throw new IOException("empty result.");
        return converted.deleteCharAt(converted.length() - separator.length()).toString();
    }

    public static String toFormatString(JsonObject json, String separator, Set<String> rmFields) throws IOException {
        if (json == null) throw new IOException("empty JsonObject.");
        StringBuilder converted = new StringBuilder();
        Set<String> set = json.keySet();
        List<String> keys = new ArrayList<String>(){{
            addAll(set);
        }};
        if (rmFields != null) keys.removeAll(rmFields);
        for (String key : keys) {
            if (fileInfoFields.contains(key)) {
                if ("putTime".equals(key)) converted.append(DatetimeUtils.datetimeOf(json.get(key).getAsLong()))
                        .append(separator); else
                if (longFields.contains(key)) converted.append(json.get(key).getAsLong()).append(separator);
                else if (intFields.contains(key)) converted.append(json.get(key).getAsInt()).append(separator);
                else converted.append(json.get(key).getAsString()).append(separator);
            } else {
                converted.append(JsonUtils.toString(json.get(key))).append(separator);
            }
        }
        if (converted.length() <= separator.length()) throw new IOException("empty result.");
        return converted.deleteCharAt(converted.length() - separator.length()).toString();
    }
}
