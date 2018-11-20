package com.qiniu.service.oss;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.model.ListResult;
import com.qiniu.model.ListV2Line;
import com.qiniu.sdk.BucketManager;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.*;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class FileLister implements Iterator<List<FileInfo>> {

    private BucketManager bucketManager;
    private String bucket;
    private String prefix;
    private String delimiter;
    private volatile String marker;
    private int limit;
    private int version;
    private volatile int retryCount;
    public volatile List<FileInfo> fileInfoList;
    public volatile QiniuException exception;

    public FileLister(BucketManager bucketManager, String bucket, String prefix, String delimiter, String marker,
                      int limit, int version, int retryCount) {
        this.bucketManager = bucketManager;
        this.bucket = bucket;
        this.prefix = prefix;
        this.delimiter = delimiter;
        this.marker = marker;
        this.limit = limit;
        this.version = version;
        this.retryCount = retryCount;
    }

    public String error() {
        if (exception != null && exception.response != null) {
            String error = exception.error();
            exception.response.close();
            return error;
        }
        return "";
    }

    /*
    v2 的 list 接口，通过 IO 流的方式返回文本信息，v1 是单次请求的结果一次性返回。
     */
    synchronized public Response list(String prefix, String delimiter, String marker, int limit) throws QiniuException {

        Response response = null;
        try {
            response = version == 2 ?
                    bucketManager.listV2(bucket, prefix, marker, limit, delimiter) :
                    bucketManager.listV1(bucket, prefix, marker, limit, delimiter);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    response = version == 2 ?
                            bucketManager.listV2(bucket, prefix, marker, limit, delimiter) :
                            bucketManager.listV1(bucket, prefix, marker, limit, delimiter);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }
        return response;
    }

    synchronized private void getListResult(String prefix, String delimiter, String marker, int limit)
            throws QiniuException {
        remove();
        Response response = list(prefix, delimiter, marker, limit);
        if (response != null) {
            if (version == 1) {
                FileListing fileListing = response.jsonToObject(FileListing.class);
                if (fileListing != null) {
                    FileInfo[] items = fileListing.items;
                    this.marker = fileListing.marker;
                    if (items.length > 0) this.fileInfoList = Arrays.asList(items);
                }
            } else if (version == 2) {
                InputStream inputStream = new BufferedInputStream(response.bodyStream());
                Reader reader = new InputStreamReader(inputStream);
                BufferedReader bufferedReader = new BufferedReader(reader);
                List<ListLine> listLines = bufferedReader.lines().parallel()
                        .filter(line -> !StringUtils.isNullOrEmpty(line))
                        .map(line -> new ListLine().fromLine(line))
                        .collect(Collectors.toList());
                this.fileInfoList = listLines.parallelStream()
                        .map(listLine -> listLine.fileInfo)
                        .collect(Collectors.toList());
                Optional<ListLine> lastListV2Line = listLines.parallelStream()
                        .max(ListLine::compareTo);
                lastListV2Line.ifPresent(listLine -> this.marker = listLine.marker);
            }
            response.close();
        }
    }

    @Override
    public boolean hasNext() {
        try {
            getListResult(prefix, delimiter, marker, limit);
        } catch (QiniuException e) {
            exception = e;
        }
        return exception != null && marker != null && !"".equals(marker) && fileInfoList != null &&
                fileInfoList.size() > 0;
    }

    @Override
    public List<FileInfo> next() {
        return fileInfoList;
    }

    @Override
    public void remove() {
        this.fileInfoList.clear();
        this.marker = "";
    }

    public class ListLine implements Comparable {

        public FileInfo fileInfo;
        public String marker = "";
        public String dir = "";

        public int compareTo(Object object) {
            com.qiniu.model.ListV2Line listV2Line = (com.qiniu.model.ListV2Line) object;
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

        public ListLine fromLine(String line) {

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
}
