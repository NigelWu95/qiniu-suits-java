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

    final static public List<String> fileFields = new ArrayList<String>(){{
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

    final static public Set<String> allFieldsSet = new HashSet<String>(){{
        addAll(fileFields);
    }};

    public static List<String> getFields(List<String> fields, List<String> rmFields) {
        if (rmFields == null) return fields;
        for (String rmField : rmFields) fields.remove(rmField);
        return fields;
    }

    public static Map<String, String> getReversedIndexMap(Map<String, String> map, List<String> rmFields) {
        Map<String, String> indexMap = new HashMap<>();
        for (Map.Entry<String, String> entry : map.entrySet()) indexMap.put(entry.getValue(), entry.getKey());
        if (rmFields == null) return indexMap;
        for (String rmField : rmFields) indexMap.remove(rmField);
        return indexMap;
    }

    public static List<String> getOrderedFields(List<String> oriFields, List<String> rmFields) {
        List<String> fields = new ArrayList<>();
        for (String fileField : fileFields) {
            if (oriFields.contains(fileField)) fields.add(fileField);
        }
        if (rmFields == null) return fields;
        for (String rmField : rmFields) fields.remove(rmField);
        return fields;
    }

    public static <T> T toPair(FileInfo fileInfo, Map<String, String> indexMap, KeyValuePair<String, T> pair)
            throws IOException {
        if (fileInfo == null || fileInfo.key == null) throw new IOException("empty fileInfo or key.");
        for (String index : indexMap.keySet()) {
            switch (index) {
                case "key": pair.put(indexMap.get(index), fileInfo.key); break;
                case "hash":
                case "etag": pair.put(indexMap.get(index), fileInfo.hash); break;
                case "size":
                case "fsize": pair.put(indexMap.get(index), fileInfo.fsize); break;
                case "lastModified":
                case "datetime": pair.put(indexMap.get(index), DatetimeUtils.stringOf(fileInfo.putTime, 10000000)); break;
                case "timestamp":
                case "putTime": pair.put(indexMap.get(index), fileInfo.putTime); break;
                case "mime":
                case "mimeType":
                case "contentType": pair.put(indexMap.get(index), fileInfo.mimeType); break;
                case "type": pair.put(indexMap.get(index), fileInfo.type); break;
                case "status": pair.put(indexMap.get(index), fileInfo.status); break;
                case "md5": pair.put(indexMap.get(index), fileInfo.md5); break;
                case "owner":
                case "endUser": if (fileInfo.endUser != null) pair.put(indexMap.get(index), fileInfo.endUser); break;
                default: throw new IOException("Qiniu fileInfo doesn't have field: " + index);
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
                case "hash":
                case "etag": pair.put(indexMap.get(index), cosObject.getETag()); break;
                case "size":
                case "fsize": pair.put(indexMap.get(index), cosObject.getSize()); break;
                case "lastModified":
                case "datetime": pair.put(indexMap.get(index), DatetimeUtils.stringOf(cosObject.getLastModified())); break;
                case "timestamp":
                case "putTime": pair.put(indexMap.get(index), cosObject.getLastModified().getTime()); break;
                case "type": pair.put(indexMap.get(index), cosObject.getStorageClass()); break;
                case "owner":
                case "endUser": if (cosObject.getOwner() != null)
                    pair.put(indexMap.get(index), cosObject.getOwner().getDisplayName()); break;
                default: throw new IOException("COSObjectSummary doesn't have field: " + index);
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
                case "hash":
                case "etag": pair.put(indexMap.get(index), ossObject.getETag()); break;
                case "size":
                case "fsize": pair.put(indexMap.get(index), ossObject.getSize()); break;
                case "lastModified":
                case "datetime": pair.put(indexMap.get(index), DatetimeUtils.stringOf(ossObject.getLastModified())); break;
                case "timestamp":
                case "putTime": pair.put(indexMap.get(index), ossObject.getLastModified().getTime()); break;
                case "type": pair.put(indexMap.get(index), ossObject.getStorageClass()); break;
                case "owner":
                case "endUser": if (ossObject.getOwner() != null)
                    pair.put(indexMap.get(index), ossObject.getOwner().getDisplayName()); break;
                default: throw new IOException("OSSObjectSummary doesn't have field: " + index);
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
                case "hash":
                case "etag": pair.put(indexMap.get(index), s3Object.getETag()); break;
                case "size":
                case "fsize": pair.put(indexMap.get(index), s3Object.getSize()); break;
                case "lastModified":
                case "datetime": pair.put(indexMap.get(index), DatetimeUtils.stringOf(s3Object.getLastModified())); break;
                case "timestamp":
                case "putTime": pair.put(indexMap.get(index), s3Object.getLastModified().getTime()); break;
                case "type": pair.put(indexMap.get(index), s3Object.getStorageClass()); break;
                case "owner":
                case "endUser": if (s3Object.getOwner() != null) pair.put(indexMap.get(index),
                        s3Object.getOwner().getDisplayName()); break;
                default: throw new IOException("S3ObjectSummary doesn't have field: " + index);
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
                case "size":
                case "fsize": pair.put(indexMap.get(index), fileItem.size); break;
                case "lastModified":
                case "datetime": pair.put(indexMap.get(index), DatetimeUtils.stringOf(fileItem.lastModified)); break;
                case "timestamp":
                case "putTime": pair.put(indexMap.get(index), fileItem.lastModified); break;
                case "mime":
                case "mimeType":
                case "contentType": pair.put(indexMap.get(index), fileItem.attribute); break;
                default: throw new IOException("Upyun fileItem doesn't have field: " + index);
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(ObsObject obsObject, Map<String, String> indexMap, KeyValuePair<String, T> pair)
            throws IOException {
        if (obsObject == null || obsObject.getObjectKey() == null) throw new IOException("empty ObsObject or key.");
        for (String index : indexMap.keySet()) {
            switch (index) {
                case "key": pair.put(indexMap.get(index), obsObject.getObjectKey()); break;
                case "hash":
                case "etag": pair.put(indexMap.get(index), obsObject.getMetadata().getEtag()); break;
                case "size":
                case "fsize": pair.put(indexMap.get(index), obsObject.getMetadata().getContentLength()); break;
                case "lastModified":
                case "datetime": pair.put(indexMap.get(index), DatetimeUtils.stringOf(obsObject.getMetadata()
                        .getLastModified())); break;
                case "timestamp":
                case "putTime": pair.put(indexMap.get(index), obsObject.getMetadata().getLastModified().getTime()); break;
                case "mime":
                case "mimeType":
                case "contentType": pair.put(indexMap.get(index), obsObject.getMetadata().getContentType()); break;
                case "type": pair.put(indexMap.get(index), obsObject.getMetadata().getObjectStorageClass().getCode()); break;
                case "md5": pair.put(indexMap.get(index), obsObject.getMetadata().getContentMd5()); break;
                case "owner":
                case "endUser": if (obsObject.getOwner() != null) pair.put(indexMap.get(index),
                        obsObject.getOwner().getId()); break;
                default: throw new IOException("ObsObject doesn't have field: " + index);
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(BosObjectSummary bosObject, Map<String, String> indexMap, KeyValuePair<String, T> pair)
            throws IOException {
        if (bosObject == null || bosObject.getKey() == null) throw new IOException("empty BosObject or key.");
        for (String index : indexMap.keySet()) {
            switch (index) {
                case "key": pair.put(indexMap.get(index), bosObject.getKey()); break;
                case "hash":
                case "etag": pair.put(indexMap.get(index), bosObject.getETag()); break;
                case "size":
                case "fsize": pair.put(indexMap.get(index), bosObject.getSize()); break;
                case "lastModified":
                case "datetime": pair.put(indexMap.get(index), DatetimeUtils.stringOf(bosObject.getLastModified())); break;
                case "timestamp":
                case "putTime": pair.put(indexMap.get(index), bosObject.getLastModified().getTime()); break;
                case "type": pair.put(indexMap.get(index), bosObject.getStorageClass()); break;
                case "owner":
                case "endUser": if (bosObject.getOwner() != null) pair.put(indexMap.get(index), bosObject.getOwner().getId()); break;
                default: throw new IOException("BosObjectSummary doesn't have field: " + index);
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(JsonObject json, Map<String, String> indexMap, KeyValuePair<String, T> pair)
            throws IOException {
        if (json == null) throw new IOException("empty JsonObject.");
        JsonElement jsonElement;
        for (String index : indexMap.keySet()) {
            jsonElement = json.get(index);
            if (jsonElement == null || jsonElement instanceof JsonNull) {
                if (!allFieldsSet.contains(index)) throw new IOException("the index: " + index + " can't be found in " + json);
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
            position = Integer.valueOf(index);
            if (items.length > position) pair.put(indexMap.get(index), items[position]);
            else throw new IOException("the index: " + index + " can't be found in " + line);
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(FileInfo fileInfo, List<String> fields, KeyValuePair<String, T> pair) throws IOException {
        if (fileInfo == null || fileInfo.key == null) throw new IOException("empty file or key.");
        for (String field : fields) {
            switch (field) {
                case "key": pair.put(field, fileInfo.key); break;
                case "hash":
                case "etag": pair.put(field, fileInfo.hash); break;
                case "size":
                case "fsize": pair.put(field, fileInfo.fsize); break;
                case "lastModified":
                case "datetime": pair.put(field, DatetimeUtils.stringOf(fileInfo.putTime, 10000000)); break;
                case "timestamp":
                case "putTime": pair.put(field, fileInfo.putTime); break;
                case "mime":
                case "mimeType":
                case "contentType": pair.put(field, fileInfo.mimeType); break;
                case "type": pair.put(field, fileInfo.type); break;
                case "status": pair.put(field, fileInfo.status); break;
                case "md5": if (fileInfo.md5 != null) pair.put(field, fileInfo.md5); break;
                case "owner":
                case "endUser": if (fileInfo.endUser != null) pair.put(field, fileInfo.endUser); break;
                default: throw new IOException("Qiniu fileInfo doesn't have field: " + field);
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
                case "hash":
                case "etag": pair.put(field, cosObject.getETag()); break;
                case "size":
                case "fsize": pair.put(field, cosObject.getSize()); break;
                case "lastModified":
                case "datetime": pair.put(field, DatetimeUtils.stringOf(cosObject.getLastModified())); break;
                case "timestamp":
                case "putTime": pair.put(field, cosObject.getLastModified().getTime()); break;
                case "type": pair.put(field, cosObject.getStorageClass()); break;
                case "owner":
                case "endUser": if (cosObject.getOwner() != null) pair.put(field, cosObject.getOwner().getDisplayName()); break;
                default: throw new IOException("COSObjectSummary doesn't have field: " + field);
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
                case "hash":
                case "etag": pair.put(field, ossObject.getETag()); break;
                case "size":
                case "fsize": pair.put(field, ossObject.getSize()); break;
                case "lastModified":
                case "datetime": pair.put(field, DatetimeUtils.stringOf(ossObject.getLastModified())); break;
                case "timestamp":
                case "putTime": pair.put(field, ossObject.getLastModified().getTime()); break;
                case "type": pair.put(field, ossObject.getStorageClass()); break;
                case "owner":
                case "endUser": if (ossObject.getOwner() != null) pair.put(field, ossObject.getOwner().getDisplayName()); break;
                default: throw new IOException("OSSObjectSummary doesn't have field: " + field);
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(S3ObjectSummary s3Object, List<String> fields, KeyValuePair<String, T> pair) throws IOException {
        if (s3Object == null || s3Object.getKey() == null) throw new IOException("empty S3ObjectSummary or key.");
        for (String field : fields) {
            switch (field) {
                case "key": pair.put(field, s3Object.getKey()); break;
                case "hash":
                case "etag": pair.put(field, s3Object.getETag()); break;
                case "size":
                case "fsize": pair.put(field, s3Object.getSize()); break;
                case "lastModified":
                case "datetime": pair.put(field, DatetimeUtils.stringOf(s3Object.getLastModified())); break;
                case "timestamp":
                case "putTime": pair.put(field, s3Object.getLastModified().getTime()); break;
                case "type": pair.put(field, s3Object.getStorageClass()); break;
                case "owner":
                case "endUser": if (s3Object.getOwner() != null) pair.put(field, s3Object.getOwner().getDisplayName()); break;
                default: throw new IOException("S3ObjectSummary doesn't have field: " + field);
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
                case "size":
                case "fsize": pair.put(field, fileItem.size); break;
                case "lastModified":
                case "datetime": pair.put(field, DatetimeUtils.stringOf(fileItem.lastModified)); break;
                case "timestamp":
                case "putTime": pair.put(field, fileItem.lastModified); break;
                case "mime":
                case "mimeType":
                case "contentType": pair.put(field, fileItem.attribute); break;
                default: throw new IOException("Upyun fileItem doesn't have field: " + field);
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
                case "hash":
                case "etag": String etag = obsObject.getMetadata().getEtag();
                if (etag.startsWith("\"")) {
                    etag = etag.endsWith("\"") ? etag.substring(1, etag.length() -1) : etag.substring(1);
                }
                pair.put(field, etag); break;
                case "size":
                case "fsize": pair.put(field, obsObject.getMetadata().getContentLength()); break;
                case "lastModified":
                case "datetime": pair.put(field, DatetimeUtils.stringOf(obsObject.getMetadata().getLastModified())); break;
                case "timestamp":
                case "putTime": pair.put(field, obsObject.getMetadata().getLastModified().getTime()); break;
                case "mime":
                case "mimeType":
                case "contentType": pair.put(field, obsObject.getMetadata().getContentType()); break;
                case "type": pair.put(field, obsObject.getMetadata().getObjectStorageClass().getCode()); break;
                case "md5": pair.put(field, obsObject.getMetadata().getContentMd5()); break;
                case "owner":
                case "endUser": if (obsObject.getOwner() != null) pair.put(field, obsObject.getOwner().getId()); break;
                default: throw new IOException("ObsObject doesn't have field: " + field);
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }

    public static <T> T toPair(BosObjectSummary bosObject, List<String> fields, KeyValuePair<String, T> pair)
            throws IOException {
        if (bosObject == null || bosObject.getKey() == null) throw new IOException("empty BosObject or key.");
        for (String field : fields) {
            switch (field) {
                case "key": pair.put(field, bosObject.getKey()); break;
                case "hash":
                case "etag": pair.put(field, bosObject.getETag()); break;
                case "size":
                case "fsize": pair.put(field, bosObject.getSize()); break;
                case "lastModified":
                case "datetime": pair.put(field, DatetimeUtils.stringOf(bosObject.getLastModified())); break;
                case "timestamp":
                case "putTime": pair.put(field, bosObject.getLastModified().getTime()); break;
                case "type": pair.put(field, bosObject.getStorageClass()); break;
                case "owner":
                case "endUser": if (bosObject.getOwner() != null) pair.put(field, bosObject.getOwner().getId()); break;
                default: throw new IOException("BosObjectSummary doesn't have field: " + field);
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
                if (!allFieldsSet.contains(field)) throw new IOException("the field: " + field + " can't be found in " + line);
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
            if (value == null || value instanceof JsonNull) {
                if (!allFieldsSet.contains(field)) throw new IOException("the field: " + field + " can't be found in " + json);
            } else {
                if (longFields.contains(field)) pair.put(field, value.getAsLong());
                else if (intFields.contains(field)) pair.put(field, value.getAsInt());
                else pair.put(field, value.getAsString());
            }
        }
        if (pair.size() == 0) throw new IOException("empty result keyValuePair.");
        return pair.getProtoEntity();
    }
}
