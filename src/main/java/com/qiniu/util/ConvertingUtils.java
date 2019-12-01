package com.qiniu.util;

import com.aliyun.oss.model.OSSObjectSummary;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.baidubce.services.bos.model.BosObjectSummary;
import com.google.gson.*;
import com.obs.services.model.ObsObject;
import com.qcloud.cos.model.COSObjectSummary;
import com.qiniu.interfaces.KeyValuePair;
import com.qiniu.sdk.FileItem;
import com.qiniu.storage.model.FileInfo;

import java.io.IOException;
import java.util.*;

public final class ConvertingUtils {

    final static public Set<String> etagFields = new HashSet<String>(){{
        add("hash");
        add("etag");
    }};

    final static public Set<String> sizeFields = new HashSet<String>(){{
        add("size");
        add("fsize");
    }};

    final static public Set<String> datetimeFields = new HashSet<String>(){{
        add("datetime");
        add("lastModified");
    }};

    final static public Set<String> timestampFields = new HashSet<String>(){{
        add("timestamp");
        add("putTime");
    }};

    final static public Set<String> mimeFields = new HashSet<String>(){{
        add("mime");
        add("mimeType");
        add("contentType");
    }};

    final static public Set<String> typeFields = new HashSet<String>(){{
        add("type");
    }};

    final static public Set<String> statusFields = new HashSet<String>(){{
        add("status");
    }};

    final static public Set<String> md5Fields = new HashSet<String>(){{
        add("md5");
    }};

    final static public Set<String> ownerFields = new HashSet<String>(){{
        add("owner");
        add("endUser");
    }};

    final static public Set<String> intFields = new HashSet<String>(){{
        addAll(statusFields);
    }};

    final static public Set<String> longFields = new HashSet<String>(){{
        addAll(sizeFields);
        addAll(timestampFields);
    }};

    final static public String defaultEtagField = "etag";
    final static public String defaultSizeField = "size";
    final static public String defaultDatetimeField = "datetime";
    final static public String defaultMimeField = "mime";
    final static public String defaultTypeField = "type";
    final static public String defaultStatusField = "status";
    final static public String defaultMd5Field = "md5";
    final static public String defaultOwnerField = "owner";

    // 为了保证字段按照设置的顺序来读取，故使用 ArrayList
    final static public List<String> defaultFileFields = new ArrayList<String>(){{
        add("key");
        add(defaultEtagField);
        add(defaultSizeField);
        add(defaultDatetimeField);
        add(defaultMimeField);
        add(defaultTypeField);
        add(defaultStatusField);
        add(defaultMd5Field);
        add(defaultOwnerField);
    }};

    // 为了保证字段按照设置的顺序来读取，故使用 ArrayList
    final static public List<String> localFileInfoFields = new ArrayList<String>(){{
        add("parent");
        add("filepath");
        add("key");
        add(defaultEtagField);
        add(defaultSizeField);
        add(defaultDatetimeField);
        add(defaultMimeField);
    }};

    final static public List<String> fileFields = new ArrayList<String>(){{
        add("parent");
        add("filepath");
        add("key");
        addAll(etagFields);
        addAll(sizeFields);
        addAll(datetimeFields);
        addAll(timestampFields);
        addAll(mimeFields);
        addAll(typeFields);
        addAll(statusFields);
        addAll(md5Fields);
        addAll(ownerFields);
        add("_id");
    }};

    final static public List<String> statFileFields = new ArrayList<String>(){{
        add("key");
        add("hash");
        add("fsize");
        add("putTime");
        add("mimeType");
        add("type");
        add("status");
        add("md5");
        add("endUser");
        add("_id");
    }};

//    final static public Set<String> allFieldsSet = new HashSet<String>(){{
//        addAll(fileFields);
//    }};

    public static List<String> getFields(List<String> fields, List<String> rmFields) {
        if (fields == null) return null;
        List<String> list = new ArrayList<>(fields);
        if (rmFields == null) return list;
        list.removeAll(rmFields);
        return list;
    }

    public static Map<String, String> getReversedIndexMap(Map<String, String> map, List<String> rmFields) {
        Map<String, String> indexMap = new HashMap<>(map.size());
        for (Map.Entry<String, String> entry : map.entrySet()) indexMap.put(entry.getValue(), entry.getKey());
        if (rmFields == null) return indexMap;
        for (String rmField : rmFields) indexMap.remove(rmField);
        return indexMap;
    }

    public static List<String> getOrderedFields(Map<String, String> indexMap, List<String> rmFields) {
        List<String> fields = new ArrayList<>(indexMap.size());
        Set<String> values = new HashSet<>(indexMap.values());
        for (String fileField : fileFields) {
            if (values.contains(fileField)) {
                fields.add(fileField);
                values.remove(fileField);
            }
        }
        fields.addAll(values);
        if (rmFields == null) return fields;
        fields.removeAll(rmFields);
        return fields;
    }

    public static <T> T toPair(FileInfo fileInfo, Map<String, String> indexMap, KeyValuePair<String, T> pair)
            throws IOException {
        if (fileInfo == null || fileInfo.key == null) throw new IOException("empty fileInfo or key.");
        for (String index : indexMap.keySet()) {
            switch (index) {
                case "key": pair.put(indexMap.get(index), fileInfo.key); break;
                case "etag": pair.put(indexMap.get(index), fileInfo.hash); break;
                case "size": pair.put(indexMap.get(index), fileInfo.fsize); break;
                case "datetime": pair.put(indexMap.get(index), DatetimeUtils.datetimeOf(fileInfo.putTime).toString()); break;
                case "timestamp": pair.put(indexMap.get(index), fileInfo.putTime); break;
                case "mime": pair.put(indexMap.get(index), fileInfo.mimeType); break;
                case "type": pair.put(indexMap.get(index), fileInfo.type); break;
                case "status": pair.put(indexMap.get(index), fileInfo.status); break;
                case "md5": pair.put(indexMap.get(index), fileInfo.md5); break;
                case "owner": if (fileInfo.endUser != null) pair.put(indexMap.get(index), fileInfo.endUser); break;
                default: throw new IOException(String.format("qiniu FileInfo doesn't have field: %s, must use fields' standard name", index));
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(COSObjectSummary cosObject, Map<String, String> indexMap, KeyValuePair<String, T> pair)
            throws IOException {
        if (cosObject == null || cosObject.getKey() == null) throw new IOException("empty cosObjectSummary or key.");
        for (String index : indexMap.keySet()) {
            switch (index) {
                case "key": pair.put(indexMap.get(index), cosObject.getKey()); break;
                case "etag": pair.put(indexMap.get(index), cosObject.getETag()); break;
                case "size": pair.put(indexMap.get(index), cosObject.getSize()); break;
                case "datetime": pair.put(indexMap.get(index), cosObject.getLastModified() == null ? null :
                    DatetimeUtils.stringOf(cosObject.getLastModified())); break;
                case "timestamp": pair.put(indexMap.get(index), cosObject.getLastModified() == null ? 0 :
                    cosObject.getLastModified().getTime()); break;
                case "type": pair.put(indexMap.get(index), cosObject.getStorageClass()); break;
                case "owner": if (cosObject.getOwner() != null)
                    pair.put(indexMap.get(index), cosObject.getOwner().getDisplayName()); break;
                default: throw new IOException(String.format("COSObject doesn't have field: %s, must use fields' standard name", index));
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(OSSObjectSummary ossObject, Map<String, String> indexMap, KeyValuePair<String, T> pair)
            throws IOException {
        if (ossObject == null || ossObject.getKey() == null) throw new IOException("empty ossObjectSummary or key.");
        for (String index : indexMap.keySet()) {
            switch (index) {
                case "key": pair.put(indexMap.get(index), ossObject.getKey()); break;
                case "etag": pair.put(indexMap.get(index), ossObject.getETag()); break;
                case "size": pair.put(indexMap.get(index), ossObject.getSize()); break;
                case "datetime": pair.put(indexMap.get(index), ossObject.getLastModified() == null ? null :
                    DatetimeUtils.stringOf(ossObject.getLastModified())); break;
                case "timestamp": pair.put(indexMap.get(index), ossObject.getLastModified() == null ? 0 :
                    ossObject.getLastModified().getTime()); break;
                case "type": pair.put(indexMap.get(index), ossObject.getStorageClass()); break;
                case "owner": if (ossObject.getOwner() != null)
                    pair.put(indexMap.get(index), ossObject.getOwner().getDisplayName()); break;
                default: throw new IOException(String.format("OSSObject doesn't have field: %s, must use fields' standard name", index));
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(S3ObjectSummary s3Object, Map<String, String> indexMap, KeyValuePair<String, T> pair)
            throws IOException {
        if (s3Object == null || s3Object.getKey() == null) throw new IOException("empty s3ObjectSummary or key.");
        for (String index : indexMap.keySet()) {
            switch (index) {
                case "key": pair.put(indexMap.get(index), s3Object.getKey()); break;
                case "etag": pair.put(indexMap.get(index), s3Object.getETag()); break;
                case "size": pair.put(indexMap.get(index), s3Object.getSize()); break;
                case "datetime": pair.put(indexMap.get(index), s3Object.getLastModified() == null ? null :
                    DatetimeUtils.stringOf(s3Object.getLastModified())); break;
                case "timestamp": pair.put(indexMap.get(index), s3Object.getLastModified() == null ? 0 :
                    s3Object.getLastModified().getTime()); break;
                case "type": pair.put(indexMap.get(index), s3Object.getStorageClass()); break;
                case "owner": if (s3Object.getOwner() != null) pair.put(indexMap.get(index),
                    s3Object.getOwner().getDisplayName()); break;
                default: throw new IOException(String.format("S3Object doesn't have field: %s, must use fields' standard name", index));
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(FileItem fileItem, Map<String, String> indexMap, KeyValuePair<String, T> pair)
            throws IOException {
        if (fileItem == null || fileItem.key == null) throw new IOException("empty fileItem or key.");
        for (String index : indexMap.keySet()) {
            switch (index) {
                case "key": pair.put(indexMap.get(index), fileItem.key); break;
                case "size": pair.put(indexMap.get(index), fileItem.size); break;
                case "datetime": pair.put(indexMap.get(index), DatetimeUtils.datetimeOf(fileItem.lastModified).toString()); break;
                case "timestamp": pair.put(indexMap.get(index), fileItem.lastModified); break;
                case "mime": pair.put(indexMap.get(index), fileItem.attribute); break;
                default: throw new IOException(String.format("upyun FileItem doesn't have field: %s, must use fields' standard name", index));
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(ObsObject obsObject, Map<String, String> indexMap, KeyValuePair<String, T> pair)
            throws IOException {
        if (obsObject == null || obsObject.getObjectKey() == null) throw new IOException("empty obsObject or key.");
        for (String index : indexMap.keySet()) {
            switch (index) {
                case "key": pair.put(indexMap.get(index), obsObject.getObjectKey()); break;
                case "etag": String etag = obsObject.getMetadata() == null ? "" : obsObject.getMetadata().getEtag();
                    if (etag.startsWith("\"")) {
                        etag = etag.endsWith("\"") ? etag.substring(1, etag.length() -1) : etag.substring(1);
                    }
                    pair.put(indexMap.get(index), etag); break;
                case "size": pair.put(indexMap.get(index), obsObject.getMetadata() == null ? 0 :
                    obsObject.getMetadata().getContentLength()); break;
                case "datetime": pair.put(indexMap.get(index), obsObject.getMetadata() == null ? null :
                    obsObject.getMetadata().getLastModified() == null ? "" :
                    DatetimeUtils.stringOf(obsObject.getMetadata().getLastModified())); break;
                case "timestamp": pair.put(indexMap.get(index), obsObject.getMetadata() == null ? 0 :
                    obsObject.getMetadata().getLastModified() == null ? 0 :
                    obsObject.getMetadata().getLastModified().getTime()); break;
                case "mime": pair.put(indexMap.get(index), obsObject.getMetadata() == null ? null :
                    obsObject.getMetadata().getContentType()); break;
                case "type": pair.put(indexMap.get(index), obsObject.getMetadata() == null ? null :
                    obsObject.getMetadata().getObjectStorageClass() == null ? "" :
                    obsObject.getMetadata().getObjectStorageClass().getCode()); break;
                case "md5": pair.put(indexMap.get(index), obsObject.getMetadata() == null ? null :
                    obsObject.getMetadata().getContentMd5()); break;
                case "owner": if (obsObject.getOwner() != null) pair.put(indexMap.get(index), obsObject.getOwner().getId()); break;
                default: throw new IOException(String.format("ObsObject doesn't have field: %s, must use fields' standard name", index));
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(BosObjectSummary bosObject, Map<String, String> indexMap, KeyValuePair<String, T> pair)
            throws IOException {
        if (bosObject == null || bosObject.getKey() == null) throw new IOException("empty bosObject or key.");
        for (String index : indexMap.keySet()) {
            switch (index) {
                case "key": pair.put(indexMap.get(index), bosObject.getKey()); break;
                case "etag": pair.put(indexMap.get(index), bosObject.getETag()); break;
                case "size": pair.put(indexMap.get(index), bosObject.getSize()); break;
                case "datetime": pair.put(indexMap.get(index), bosObject.getLastModified() == null ? null :
                    DatetimeUtils.stringOf(bosObject.getLastModified())); break;
                case "timestamp": pair.put(indexMap.get(index), bosObject.getLastModified() == null ? 0 :
                    bosObject.getLastModified().getTime()); break;
                case "type": pair.put(indexMap.get(index), bosObject.getStorageClass()); break;
                case "owner": if (bosObject.getOwner() != null) pair.put(indexMap.get(index), bosObject.getOwner().getId()); break;
                default: throw new IOException(String.format("BosObject doesn't have field: %s, must use fields' standard name", index));
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(JsonObject json, Map<String, String> indexMap, KeyValuePair<String, T> pair) throws IOException {
        if (json == null) throw new IOException("empty jsonObject.");
        JsonElement jsonElement;
        for (String index : indexMap.keySet()) {
            jsonElement = json.get(index);
            // JsonUtils.toString(null) 和 JsonUtils.toString(JsonNull.INSTANCE) 均为 null
            if (jsonElement instanceof JsonPrimitive) {
                JsonPrimitive primitive = jsonElement.getAsJsonPrimitive();
                if (primitive.isBoolean()) pair.put(indexMap.get(index), JsonUtils.fromJson(jsonElement, Boolean.class));
                else if (primitive.isNumber()) pair.put(indexMap.get(index), JsonUtils.fromJson(jsonElement, Long.class));
                else pair.put(indexMap.get(index), String.valueOf(jsonElement));
            } else {
                try {
                    pair.put(indexMap.get(index), JsonUtils.toString(jsonElement));
                } catch (JsonSyntaxException e) {
                    pair.put(indexMap.get(index), String.valueOf(jsonElement));
                }
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(String line, Map<String, String> indexMap, KeyValuePair<String, T> pair) throws IOException {
        if (line == null) throw new IOException("empty json line.");
        JsonObject parsed = new JsonParser().parse(line).getAsJsonObject();
        return toPair(parsed, indexMap, pair);
    }

    public static <T> T toPair(String line, String separator, Map<String, String> indexMap, KeyValuePair<String, T> pair)
            throws IOException {
        if (line == null) throw new IOException("empty string line.");
        String[] items = line.split(separator);
        int position;
        for (String index : indexMap.keySet()) {
            position = Integer.parseInt(index);
            if (items.length > position) pair.put(indexMap.get(index), items[position]);
            else throw new IOException(String.format("the index: %s can't be found in %s", index, line));
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(com.qiniu.model.local.FileInfo fileInfo, Map<String, String> indexMap, KeyValuePair<String, T> pair)
            throws IOException {
        if (fileInfo == null || fileInfo.filepath == null) throw new IOException("empty fileInfo or filepath.");
        for (String index : indexMap.keySet()) {
            switch (index) {
                case "parent": if (fileInfo.parentPath != null) pair.put(indexMap.get(index), fileInfo.parentPath); break;
                case "filepath": pair.put(indexMap.get(index), fileInfo.filepath); break;
                case "key": if (fileInfo.key != null) pair.put(indexMap.get(index), fileInfo.key); break;
                case "etag": if (fileInfo.etag != null) pair.put(indexMap.get(index), fileInfo.etag); break;
                case "size": pair.put(indexMap.get(index), fileInfo.length); break;
                case "datetime": pair.put(indexMap.get(index), DatetimeUtils.datetimeOf(fileInfo.timestamp).toString()); break;
                case "timestamp": pair.put(indexMap.get(index), fileInfo.timestamp); break;
                case "mime": if (fileInfo.mime != null) pair.put(indexMap.get(index), fileInfo.mime); break;
                default: throw new IOException(String.format("local FileInfo doesn't have field: %s, must use fields' standard name", index));
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(FileInfo fileInfo, List<String> fields, KeyValuePair<String, T> pair) throws IOException {
        if (fileInfo == null || fileInfo.key == null) throw new IOException("empty fileInfo or key.");
        for (String field : fields) {
            switch (field) {
                case "key": pair.put(field, fileInfo.key); break;
                case "etag": pair.put(field, fileInfo.hash); break;
                case "size": pair.put(field, fileInfo.fsize); break;
                case "datetime": pair.put(field, DatetimeUtils.datetimeOf(fileInfo.putTime).toString()); break;
                case "timestamp": pair.put(field, fileInfo.putTime); break;
                case "mime": pair.put(field, fileInfo.mimeType); break;
                case "type": pair.put(field, fileInfo.type); break;
                case "status": pair.put(field, fileInfo.status); break;
                case "md5": if (fileInfo.md5 != null) pair.put(field, fileInfo.md5); break;
                case "owner": if (fileInfo.endUser != null) pair.put(field, fileInfo.endUser); break;
                default: throw new IOException(String.format("qiniu FileInfo doesn't have field: %s, must use fields' standard name", field));
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(COSObjectSummary cosObject, List<String> fields, KeyValuePair<String, T> pair) throws IOException {
        if (cosObject == null || cosObject.getKey() == null) throw new IOException("empty cosObjectSummary or key.");
        for (String field : fields) {
            switch (field) {
                case "key": pair.put(field, cosObject.getKey()); break;
                case "etag": pair.put(field, cosObject.getETag()); break;
                case "size": pair.put(field, cosObject.getSize()); break;
                case "datetime": pair.put(field, cosObject.getLastModified() == null ? "" :
                    DatetimeUtils.stringOf(cosObject.getLastModified())); break;
                case "timestamp": pair.put(field, cosObject.getLastModified() == null ? 0 :
                    cosObject.getLastModified().getTime()); break;
                case "type": pair.put(field, cosObject.getStorageClass()); break;
                case "owner": if (cosObject.getOwner() != null) pair.put(field, cosObject.getOwner().getDisplayName()); break;
                default: throw new IOException(String.format("COSObject doesn't have field: %s, must use fields' standard name", field));
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(OSSObjectSummary ossObject, List<String> fields, KeyValuePair<String, T> pair) throws IOException {
        if (ossObject == null || ossObject.getKey() == null) throw new IOException("empty ossObjectSummary or key.");
        for (String field : fields) {
            switch (field) {
                case "key": pair.put(field, ossObject.getKey()); break;
                case "etag": pair.put(field, ossObject.getETag()); break;
                case "size": pair.put(field, ossObject.getSize()); break;
                case "datetime": pair.put(field, ossObject.getLastModified() == null ? "" :
                    DatetimeUtils.stringOf(ossObject.getLastModified())); break;
                case "timestamp": pair.put(field, ossObject.getLastModified() == null ? 0 :
                    ossObject.getLastModified().getTime()); break;
                case "type": pair.put(field, ossObject.getStorageClass()); break;
                case "owner": if (ossObject.getOwner() != null) pair.put(field, ossObject.getOwner().getDisplayName()); break;
                default: throw new IOException(String.format("OSSObject doesn't have field: %s, must use fields' standard name", field));
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(S3ObjectSummary s3Object, List<String> fields, KeyValuePair<String, T> pair) throws IOException {
        if (s3Object == null || s3Object.getKey() == null) throw new IOException("empty s3ObjectSummary or key.");
        for (String field : fields) {
            switch (field) {
                case "key": pair.put(field, s3Object.getKey()); break;
                case "etag": pair.put(field, s3Object.getETag()); break;
                case "size": pair.put(field, s3Object.getSize()); break;
                case "datetime": pair.put(field, s3Object.getLastModified() == null ? "" :
                    DatetimeUtils.stringOf(s3Object.getLastModified())); break;
                case "timestamp": pair.put(field, s3Object.getLastModified() == null ? 0 :
                    s3Object.getLastModified().getTime()); break;
                case "type": pair.put(field, s3Object.getStorageClass()); break;
                case "owner": if (s3Object.getOwner() != null) pair.put(field, s3Object.getOwner().getDisplayName()); break;
                default: throw new IOException(String.format("S3Object doesn't have field: %s, must use fields' standard name", field));
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(FileItem fileItem, List<String> fields, KeyValuePair<String, T> pair) throws IOException {
        if (fileItem == null || fileItem.key == null) throw new IOException("empty fileItem or key.");
        for (String field : fields) {
            switch (field) {
                case "key": pair.put(field, fileItem.key); break;
                case "size": pair.put(field, fileItem.size); break;
                case "datetime": pair.put(field, DatetimeUtils.datetimeOf(fileItem.lastModified).toString()); break;
                case "timestamp": pair.put(field, fileItem.lastModified); break;
                case "mime": pair.put(field, fileItem.attribute); break;
                default: throw new IOException(String.format("upyun FileItem doesn't have field: %s, must use fields' standard name", field));
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(ObsObject obsObject, List<String> fields, KeyValuePair<String, T> pair)
            throws IOException {
        if (obsObject == null || obsObject.getObjectKey() == null) throw new IOException("empty fileItem or key.");
        for (String field : fields) {
            switch (field) {
                case "key": pair.put(field, obsObject.getObjectKey()); break;
                case "etag": String etag = obsObject.getMetadata() == null ? "" : obsObject.getMetadata().getEtag();
                    if (etag.startsWith("\"")) {
                        etag = etag.endsWith("\"") ? etag.substring(1, etag.length() -1) : etag.substring(1);
                    }
                    pair.put(field, etag); break;
                case "size": pair.put(field, obsObject.getMetadata() == null ? 0 :
                    obsObject.getMetadata().getContentLength()); break;
                case "datetime": pair.put(field, obsObject.getMetadata() == null ? "" :
                    obsObject.getMetadata().getLastModified() == null ? "" :
                    DatetimeUtils.stringOf(obsObject.getMetadata().getLastModified())); break;
                case "timestamp": pair.put(field, obsObject.getMetadata() == null ? 0 :
                    obsObject.getMetadata().getLastModified() == null ? 0 :
                    obsObject.getMetadata().getLastModified().getTime()); break;
                case "mime": pair.put(field, obsObject.getMetadata() == null ? "" :
                    obsObject.getMetadata().getContentType()); break;
                case "type": pair.put(field, obsObject.getMetadata() == null ? "" :
                    obsObject.getMetadata().getObjectStorageClass() == null ? "" :
                    obsObject.getMetadata().getObjectStorageClass().getCode()); break;
                case "md5": pair.put(field, obsObject.getMetadata() == null ? "" :
                    obsObject.getMetadata().getContentMd5()); break;
                case "owner": if (obsObject.getOwner() != null) pair.put(field, obsObject.getOwner().getId()); break;
                default: throw new IOException(String.format("ObsObject doesn't have field: %s, must use fields' standard name", field));
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(BosObjectSummary bosObject, List<String> fields, KeyValuePair<String, T> pair)
            throws IOException {
        if (bosObject == null || bosObject.getKey() == null) throw new IOException("empty bosObject or key.");
        for (String field : fields) {
            switch (field) {
                case "key": pair.put(field, bosObject.getKey()); break;
                case "etag": pair.put(field, bosObject.getETag()); break;
                case "size": pair.put(field, bosObject.getSize()); break;
                case "datetime": pair.put(field, bosObject.getLastModified() == null ? "" :
                        DatetimeUtils.stringOf(bosObject.getLastModified())); break;
                case "timestamp": pair.put(field, bosObject.getLastModified() == null ? 0 :
                        bosObject.getLastModified().getTime()); break;
                case "type": pair.put(field, bosObject.getStorageClass()); break;
                case "owner": if (bosObject.getOwner() != null) pair.put(field, bosObject.getOwner().getId()); break;
                default: throw new IOException(String.format("BosObject doesn't have field: %s, must use fields' standard name", field));
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(Map<String, String> line, List<String> fields, KeyValuePair<String, T> pair) throws IOException {
        if (line == null) throw new IOException("empty string map.");
        String value;
        for (String field : fields) {
            value = line.get(field);
            if (value != null) {
                if (longFields.contains(field)) pair.put(field, Long.valueOf(value));
                else if (intFields.contains(field)) pair.put(field, Integer.valueOf(value));
                else pair.put(field, value);
            } else {
                pair.put(field, KeyValuePair.EMPTY);
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(JsonObject json, List<String> fields, KeyValuePair<String, T> pair) throws IOException {
        if (json == null) throw new IOException("empty JsonObject.");
        JsonElement value;
        for (String field : fields) {
            value = json.get(field);
            if (longFields.contains(field)) pair.put(field, value.getAsLong());
            else if (intFields.contains(field)) pair.put(field, value.getAsInt());
            else if (value instanceof JsonPrimitive) {
                JsonPrimitive primitive = value.getAsJsonPrimitive();
                if (primitive.isBoolean()) pair.put(field, JsonUtils.fromJson(value, Boolean.class));
                else if (primitive.isNumber()) pair.put(field, JsonUtils.fromJson(value, Long.class));
                else pair.put(field, String.valueOf(value));
            } else {
                try {
                    pair.put(field, JsonUtils.toString(value));
                } catch (JsonSyntaxException e) {
                    pair.put(field, String.valueOf(value));
                }
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(com.qiniu.model.local.FileInfo fileInfo, List<String> fields, KeyValuePair<String, T> pair) throws IOException {
        if (fileInfo == null || (fileInfo.filepath == null && fileInfo.key == null)) throw new IOException("empty fileInfo or empty path and key.");
        for (String field : fields) {
            switch (field) {
                case "parent": if (fileInfo.parentPath != null) pair.put(field, fileInfo.parentPath); break;
                case "filepath": pair.put(field, fileInfo.filepath); break;
                case "key": pair.put(field, fileInfo.key); break;
                case "etag": if (fileInfo.etag != null) pair.put(field, fileInfo.etag); break;
                case "size": pair.put(field, fileInfo.length); break;
                case "datetime": pair.put(field, DatetimeUtils.datetimeOf(fileInfo.timestamp).toString()); break;
                case "timestamp": pair.put(field, fileInfo.timestamp); break;
                case "mime": if (fileInfo.mime != null) pair.put(field, fileInfo.mime); break;
                default: throw new IOException(String.format("local FileInfo doesn't have field: %s, must use fields' standard name", field));
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static String toStringWithIndent(FileInfo fileInfo, List<String> fields) throws IOException {
        if (fileInfo == null || fileInfo.key == null) throw new IOException("empty fileInfo or key.");
        StringBuilder builder = new StringBuilder();
        StringBuilder parentPath = new StringBuilder();
        for (String field : fields) {
            switch (field) {
                case "key":
                    if (fileInfo.key == null) throw new IOException("object key is empty");
                    int num = fileInfo.key.split(FileUtils.pathSeparator).length;
                    if (num > 1) parentPath.append(fileInfo.key, 0, fileInfo.key.lastIndexOf(FileUtils.pathSeparator));
                    else parentPath.append("/");
                    StringBuilder stringBuilder = new StringBuilder();
                    for (int j = 0; j < num; j++) stringBuilder.append("\t");
                    stringBuilder.append(fileInfo.key.replace(parentPath, "").substring(1));
                    builder.append(stringBuilder.toString()).append("\t");
                    parentPath.append("-||-"); break;
                case "etag": builder.append(fileInfo.hash); break;
                case "size": builder.append(fileInfo.fsize); break;
                case "datetime": builder.append(DatetimeUtils.datetimeOf(fileInfo.putTime).toString()); break;
                case "timestamp": builder.append(fileInfo.putTime); break;
                case "mime": builder.append(fileInfo.mimeType); break;
                case "type": builder.append(fileInfo.type); break;
                case "status": builder.append(fileInfo.status); break;
                case "md5": if (fileInfo.md5 != null) builder.append(fileInfo.md5); break;
                case "owner": if (fileInfo.endUser != null) builder.append(fileInfo.endUser); break;
                default: throw new IOException(String.format("qiniu FileInfo doesn't have field: %s, must use fields' standard name", field));
            }
        }
        if (builder.length() == 0) throw new IOException("empty result string.");
        return parentPath.append(builder.deleteCharAt(builder.length() - 1)).toString();
    }

    public static String toStringWithIndent(COSObjectSummary cosObject, List<String> fields) throws IOException {
        if (cosObject == null || cosObject.getKey() == null) throw new IOException("empty cosObjectSummary or key.");
        StringBuilder builder = new StringBuilder();
        StringBuilder parentPath = new StringBuilder();
        for (String field : fields) {
            switch (field) {
                case "key": String key = cosObject.getKey();
                    if (key == null) throw new IOException("object key is empty");
                    int num = key.split(FileUtils.pathSeparator).length;
                    if (num > 1) parentPath.append(key, 0, key.lastIndexOf(FileUtils.pathSeparator));
                    else parentPath.append("/");
                    StringBuilder stringBuilder = new StringBuilder();
                    for (int j = 0; j < num; j++) stringBuilder.append("\t");
                    stringBuilder.append(key.replace(parentPath, "").substring(1));
                    builder.append(stringBuilder.toString()).append("\t");
                    parentPath.append("-||-"); break;
                case "etag": builder.append(cosObject.getETag()); break;
                case "size": builder.append(cosObject.getSize()); break;
                case "datetime": builder.append(cosObject.getLastModified() == null ? "" :
                        DatetimeUtils.stringOf(cosObject.getLastModified())); break;
                case "timestamp": builder.append(cosObject.getLastModified() == null ? 0 :
                        cosObject.getLastModified().getTime()); break;
                case "type": builder.append(cosObject.getStorageClass()); break;
                case "owner": if (cosObject.getOwner() != null) builder.append(cosObject.getOwner().getDisplayName()); break;
                default: throw new IOException(String.format("COSObject doesn't have field: %s, must use fields' standard name", field));
            }
        }
        if (builder.length() == 0) throw new IOException("empty result string.");
        return parentPath.append(builder.deleteCharAt(builder.length() - 1)).toString();
    }

    public static String toStringWithIndent(OSSObjectSummary ossObject, List<String> fields) throws IOException {
        if (ossObject == null || ossObject.getKey() == null) throw new IOException("empty ossObjectSummary or key.");
        StringBuilder builder = new StringBuilder();
        StringBuilder parentPath = new StringBuilder();
        for (String field : fields) {
            switch (field) {
                case "key": String key = ossObject.getKey();
                    if (key == null) throw new IOException("object key is empty");
                    int num = key.split(FileUtils.pathSeparator).length;
                    if (num > 1) parentPath.append(key, 0, key.lastIndexOf(FileUtils.pathSeparator));
                    else parentPath.append("/");
                    StringBuilder stringBuilder = new StringBuilder();
                    for (int j = 0; j < num; j++) stringBuilder.append("\t");
                    stringBuilder.append(key.replace(parentPath, "").substring(1));
                    builder.append(stringBuilder.toString()).append("\t");
                    parentPath.append("-||-"); break;
                case "etag": builder.append(ossObject.getETag()); break;
                case "size": builder.append(ossObject.getSize()); break;
                case "datetime": builder.append(ossObject.getLastModified() == null ? "" :
                        DatetimeUtils.stringOf(ossObject.getLastModified())); break;
                case "timestamp": builder.append(ossObject.getLastModified() == null ? 0 :
                        ossObject.getLastModified().getTime()); break;
                case "type": builder.append(ossObject.getStorageClass()); break;
                case "owner": if (ossObject.getOwner() != null) builder.append(ossObject.getOwner().getDisplayName()); break;
                default: throw new IOException(String.format("OSSObject doesn't have field: %s, must use fields' standard name", field));
            }
        }
        if (builder.length() == 0) throw new IOException("empty result string.");
        return parentPath.append(builder.deleteCharAt(builder.length() - 1)).toString();
    }

    public static String toStringWithIndent(S3ObjectSummary s3Object, List<String> fields) throws IOException {
        if (s3Object == null || s3Object.getKey() == null) throw new IOException("empty s3ObjectSummary or key.");
        StringBuilder builder = new StringBuilder();
        StringBuilder parentPath = new StringBuilder();
        for (String field : fields) {
            switch (field) {
                case "key": String key = s3Object.getKey();
                    if (key == null) throw new IOException("object key is empty");
                    int num = key.split(FileUtils.pathSeparator).length;
                    if (num > 1) parentPath.append(key, 0, key.lastIndexOf(FileUtils.pathSeparator));
                    else parentPath.append("/");
                    StringBuilder stringBuilder = new StringBuilder();
                    for (int j = 0; j < num; j++) stringBuilder.append("\t");
                    stringBuilder.append(key.replace(parentPath, "").substring(1));
                    builder.append(stringBuilder.toString()).append("\t");
                    parentPath.append("-||-"); break;
                case "etag": builder.append(s3Object.getETag()); break;
                case "size": builder.append(s3Object.getSize()); break;
                case "datetime": builder.append(s3Object.getLastModified() == null ? "" :
                        DatetimeUtils.stringOf(s3Object.getLastModified())); break;
                case "timestamp": builder.append(s3Object.getLastModified() == null ? 0 :
                        s3Object.getLastModified().getTime()); break;
                case "type": builder.append(s3Object.getStorageClass()); break;
                case "owner": if (s3Object.getOwner() != null) builder.append(s3Object.getOwner().getDisplayName()); break;
                default: throw new IOException(String.format("S3Object doesn't have field: %s, must use fields' standard name", field));
            }
        }
        if (builder.length() == 0) throw new IOException("empty result string.");
        return parentPath.append(builder.deleteCharAt(builder.length() - 1)).toString();
    }

    public static String toStringWithIndent(FileItem fileItem, List<String> fields) throws IOException {
        if (fileItem == null || fileItem.key == null) throw new IOException("empty fileItem or key.");
        StringBuilder builder = new StringBuilder();
        StringBuilder parentPath = new StringBuilder();
        for (String field : fields) {
            switch (field) {
                case "key": if (fileItem.key == null) throw new IOException("object key is empty");
                    int num = fileItem.key.split(FileUtils.pathSeparator).length;
                    if (num > 1) parentPath.append(fileItem.key, 0, fileItem.key.lastIndexOf(FileUtils.pathSeparator));
                    else parentPath.append("/");
                    StringBuilder stringBuilder = new StringBuilder();
                    for (int j = 0; j < num; j++) stringBuilder.append("\t");
                    stringBuilder.append(fileItem.key.replace(parentPath, "").substring(1));
                    builder.append(stringBuilder.toString()).append("\t");
                    parentPath.append("-||-"); break;
                case "size": builder.append(fileItem.size); break;
                case "datetime": builder.append(DatetimeUtils.datetimeOf(fileItem.lastModified).toString()); break;
                case "timestamp": builder.append(fileItem.lastModified); break;
                case "mime": builder.append(fileItem.attribute); break;
                default: throw new IOException(String.format("upyun FileItem doesn't have field: %s, must use fields' standard name", field));
            }
        }
        if (builder.length() == 0) throw new IOException("empty result string.");
        return parentPath.append(builder.deleteCharAt(builder.length() - 1)).toString();
    }

    public static String toStringWithIndent(ObsObject obsObject, List<String> fields) throws IOException {
        if (obsObject == null || obsObject.getObjectKey() == null) throw new IOException("empty fileItem or key.");
        StringBuilder builder = new StringBuilder();
        StringBuilder parentPath = new StringBuilder();
        for (String field : fields) {
            switch (field) {
                case "key": String key = obsObject.getObjectKey();
                    if (key == null) throw new IOException("object key is empty");
                    int num = key.split(FileUtils.pathSeparator).length;
                    if (num > 1) parentPath.append(key, 0, key.lastIndexOf(FileUtils.pathSeparator));
                    else parentPath.append("/");
                    StringBuilder stringBuilder = new StringBuilder();
                    for (int j = 0; j < num; j++) stringBuilder.append("\t");
                    stringBuilder.append(key.replace(parentPath, "").substring(1));
                    builder.append(stringBuilder.toString()).append("\t");
                    parentPath.append("-||-"); break;
                case "etag": String etag = obsObject.getMetadata() == null ? "" : obsObject.getMetadata().getEtag();
                    if (etag.startsWith("\"")) {
                        etag = etag.endsWith("\"") ? etag.substring(1, etag.length() -1) : etag.substring(1);
                    }
                    builder.append(etag); break;
                case "size": builder.append(obsObject.getMetadata() == null ? 0 :
                        obsObject.getMetadata().getContentLength()); break;
                case "datetime": builder.append(obsObject.getMetadata() == null ? "" :
                        obsObject.getMetadata().getLastModified() == null ? "" :
                                DatetimeUtils.stringOf(obsObject.getMetadata().getLastModified())); break;
                case "timestamp": builder.append(obsObject.getMetadata() == null ? 0 :
                        obsObject.getMetadata().getLastModified() == null ? 0 :
                                obsObject.getMetadata().getLastModified().getTime()); break;
                case "mime": builder.append(obsObject.getMetadata() == null ? "" :
                        obsObject.getMetadata().getContentType()); break;
                case "type": builder.append(obsObject.getMetadata() == null ? "" :
                        obsObject.getMetadata().getObjectStorageClass() == null ? "" :
                                obsObject.getMetadata().getObjectStorageClass().getCode()); break;
                case "md5": builder.append(obsObject.getMetadata() == null ? "" :
                        obsObject.getMetadata().getContentMd5()); break;
                case "owner": if (obsObject.getOwner() != null) builder.append(obsObject.getOwner().getId()); break;
                default: throw new IOException(String.format("ObsObject doesn't have field: %s, must use fields' standard name", field));
            }
        }
        if (builder.length() == 0) throw new IOException("empty result string.");
        return parentPath.append(builder.deleteCharAt(builder.length() - 1)).toString();
    }

    public static String toStringWithIndent(BosObjectSummary bosObject, List<String> fields) throws IOException {
        if (bosObject == null || bosObject.getKey() == null) throw new IOException("empty bosObject or key.");
        StringBuilder builder = new StringBuilder();
        StringBuilder parentPath = new StringBuilder();
        for (String field : fields) {
            switch (field) {
                case "key": String key = bosObject.getKey();
                    if (key == null) throw new IOException("object key is empty");
                    int num = key.split(FileUtils.pathSeparator).length;
                    if (num > 1) parentPath.append(key, 0, key.lastIndexOf(FileUtils.pathSeparator));
                    else parentPath.append("/");
                    StringBuilder stringBuilder = new StringBuilder();
                    for (int j = 0; j < num; j++) stringBuilder.append("\t");
                    stringBuilder.append(key.replace(parentPath, "").substring(1));
                    builder.append(stringBuilder.toString()).append("\t");
                    parentPath.append("-||-"); break;
                case "etag": builder.append(bosObject.getETag()); break;
                case "size": builder.append(bosObject.getSize()); break;
                case "datetime": builder.append(bosObject.getLastModified() == null ? "" :
                        DatetimeUtils.stringOf(bosObject.getLastModified())); break;
                case "timestamp": builder.append(bosObject.getLastModified() == null ? 0 :
                        bosObject.getLastModified().getTime()); break;
                case "type": builder.append(bosObject.getStorageClass()); break;
                case "owner": if (bosObject.getOwner() != null) builder.append(bosObject.getOwner().getId()); break;
                default: throw new IOException(String.format("BosObject doesn't have field: %s, must use fields' standard name", field));
            }
        }
        if (builder.length() == 0) throw new IOException("empty result string.");
        return parentPath.append(builder.deleteCharAt(builder.length() - 1)).toString();
    }

    public static String toStringWithIndent(Map<String, String> line, List<String> fields) throws IOException {
        if (line == null) throw new IOException("empty string map.");
        StringBuilder builder = new StringBuilder();
        StringBuilder parentPath = new StringBuilder();
        String value;
        for (String field : fields) {
            value = line.get(field);
            if ("key".equals(field)) {
                if (value == null) throw new IOException("object key is empty");
                int num = value.split(FileUtils.pathSeparator).length;
                if (num > 1) parentPath.append(value, 0, value.lastIndexOf(FileUtils.pathSeparator));
                else parentPath.append("/");
                StringBuilder stringBuilder = new StringBuilder();
                for (int j = 0; j < num; j++) stringBuilder.append("\t");
                stringBuilder.append(value.replace(parentPath, "").substring(1));
                builder.append(stringBuilder.toString()).append("\t");
                parentPath.append("-||-");
            } else {
                builder.append(value).append("\t");
            }
        }
        if (builder.length() == 0) throw new IOException("empty result string.");
        return parentPath.append(builder.deleteCharAt(builder.length() - 1)).toString();
    }

    public static String toStringWithIndent(com.qiniu.model.local.FileInfo fileInfo, List<String> fields, int initPathSize) throws IOException {
        if (fileInfo == null || fileInfo.filepath == null) throw new IOException("empty fileInfo or empty path.");
        StringBuilder builder = new StringBuilder();
        StringBuilder parentPath = new StringBuilder();
        if (fileInfo.parentPath != null) {
            int num = fileInfo.parentPath.split(FileUtils.pathSeparator).length - initPathSize;
            for (String field : fields) {
                switch (field) {
                    case "parent": parentPath.append(fileInfo.parentPath).append("-||-"); break;
                    case "filepath": StringBuilder stringBuilder = new StringBuilder();
                        for (int j = 0; j < num; j++) stringBuilder.append("\t");
                        stringBuilder.append(fileInfo.filepath.replace(fileInfo.parentPath, "").substring(1));
                        builder.append(stringBuilder.toString()).append("\t"); break;
                    case "key": if (fileInfo.key != null) builder.append(fileInfo.key).append("\t"); break;
                    case "etag": if (fileInfo.etag != null) builder.append(fileInfo.etag).append("\t"); break;
                    case "size": builder.append(fileInfo.length).append("\t"); break;
                    case "datetime": builder.append(DatetimeUtils.datetimeOf(fileInfo.timestamp).toString()).append("\t"); break;
                    case "timestamp": builder.append(fileInfo.timestamp).append("\t"); break;
                    case "mime": if (fileInfo.mime != null) builder.append(fileInfo.mime).append("\t"); break;
                    default: throw new IOException(String.format("local FileInfo doesn't have field: %s, must use fields' standard name", field));
                }
            }
            if (builder.length() == 0) throw new IOException("empty result string.");
            return parentPath.append(builder.deleteCharAt(builder.length() - 1 - 1)).toString();
        } else {
            throw new IOException("no parent path to parse.");
        }
    }

    public static String toStringWithIndent(JsonObject json, List<String> fields) throws IOException {
        if (json == null) throw new IOException("empty jsonObject.");
        JsonElement jsonElement;
        StringBuilder builder = new StringBuilder();
        StringBuilder parentPath = new StringBuilder();
        for (String field : fields) {
            jsonElement = json.get(field);
            if ("key".equals(field)) {
                String key = JsonUtils.toString(jsonElement);
                if (key == null) throw new IOException("object key is empty");
                int num = key.split(FileUtils.pathSeparator).length;
                if (num > 1) parentPath.append(key, 0, key.lastIndexOf(FileUtils.pathSeparator));
                else parentPath.append("/");
                StringBuilder stringBuilder = new StringBuilder();
                for (int j = 0; j < num; j++) stringBuilder.append("\t");
                stringBuilder.append(key.replace(parentPath, "").substring(1));
                builder.append(stringBuilder.toString()).append("\t");
                parentPath.append("-||-");
            } else {
                builder.append(JsonUtils.toString(jsonElement)).append("\t");
            }
        }
        if (builder.length() == 0) throw new IOException("empty result string.");
        return parentPath.append(builder.deleteCharAt(builder.length() - 1)).toString();
    }

    public static String toStringWithIndent(String line, List<String> fields) throws IOException {
        if (line == null) throw new IOException("empty json line.");
        JsonObject parsed = new JsonParser().parse(line).getAsJsonObject();
        return toStringWithIndent(parsed, fields);
    }
}
