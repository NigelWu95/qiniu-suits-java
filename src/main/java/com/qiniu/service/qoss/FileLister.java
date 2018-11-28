package com.qiniu.service.qoss;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
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
    private String marker;
    private int limit;
    private int version;
    private int retryCount;
    private List<FileInfo> fileInfoList;
    public QiniuException exception;

    public FileLister(BucketManager bucketManager, String bucket, String prefix, String delimiter, String marker,
                      int limit, int version, int retryCount) throws QiniuException {
        this.bucketManager = bucketManager;
        this.bucket = bucket;
        this.prefix = prefix;
        this.delimiter = delimiter;
        this.marker = marker;
        this.limit = limit;
        this.version = version;
        this.retryCount = retryCount;
        this.fileInfoList = getListResult(prefix, delimiter, marker, limit);
    }

    public String error() {
        if (exception != null && exception.response != null) {
            String error = exception.error();
            exception.response.close();
            return error;
        }
        return "";
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public String getMarker() {
        return marker;
    }

    public void setMarker(String marker) {
        this.marker = marker;
    }

    public int getLimit() {
        return limit;
    }

    public List<FileInfo> getFileInfoList() {
        return fileInfoList;
    }

    /**
     * v2 的 list 接口，通过 IO 流的方式返回文本信息，v1 是单次请求的结果一次性返回。
     * @param prefix
     * @param delimiter
     * @param marker
     * @param limit
     * @return
     * @throws QiniuException
     */
    public Response list(String prefix, String delimiter, String marker, int limit) throws QiniuException {

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

    private List<FileInfo> getListResult(String prefix, String delimiter, String marker, int limit)
            throws QiniuException {
        List<FileInfo> resultList = new ArrayList<>();
        Response response = list(prefix, delimiter, marker, limit);
        if (response != null) {
            if (version == 1) {
                FileListing fileListing = response.jsonToObject(FileListing.class);
                if (fileListing != null) {
                    FileInfo[] items = fileListing.items;
                    this.marker = fileListing.marker;
                    if (items.length > 0) resultList = Arrays.asList(items);
                }
            } else if (version == 2) {
                InputStream inputStream = new BufferedInputStream(response.bodyStream());
                Reader reader = new InputStreamReader(inputStream);
                BufferedReader bufferedReader = new BufferedReader(reader);
                List<String> lines = bufferedReader.lines().collect(Collectors.toList());
                List<ListLine> listLines = lines.parallelStream()
                        .filter(line -> !StringUtils.isNullOrEmpty(line))
                        .map(line -> new ListLine().fromLine(line))
                        .collect(Collectors.toList());
                if (listLines.size() < lines.size()) {
                    throw new QiniuException(null, "convert line to file info error.");
                }
                resultList = listLines.parallelStream()
                        .map(listLine -> listLine.fileInfo)
                        .collect(Collectors.toList());
                Optional<ListLine> lastListLine = listLines.parallelStream()
                        .max(ListLine::compareTo);
                this.marker = lastListLine.map(listLine -> listLine.marker).orElse("");
            }
            response.close();
        }

        return resultList;
    }

    @Override
    public boolean hasNext() {
        return fileInfoList != null && fileInfoList.size() > 0;
    }

    @Override
    public List<FileInfo> next() {
        List<FileInfo> current = fileInfoList == null ? new ArrayList<>() : fileInfoList;
        try {
            fileInfoList = (marker == null || "".equals(marker)) ? new ArrayList<>() :
                    getListResult(prefix, delimiter, marker, limit);
        } catch (QiniuException e) {
            fileInfoList = null;
            exception = e;
        }
        return current;
    }

    public class ListLine implements Comparable {

        public FileInfo fileInfo;
        public String marker = "";
        public String dir = "";

        public boolean isDeleted() {
            return (fileInfo == null && StringUtils.isNullOrEmpty(dir));
        }

        public int compareTo(Object object) {
            ListLine listLine = (ListLine) object;
            if (listLine.fileInfo == null && this.fileInfo == null) {
                return 0;
            } else if (this.fileInfo == null) {
                if (!"".equals(marker)) {
                    String markerJson = new String(UrlSafeBase64.decode(marker));
                    String key = JsonConvertUtils.fromJson(markerJson, JsonObject.class).get("k").getAsString();
                    return key.compareTo(listLine.fileInfo.key);
                }
                return 1;
            } else if (listLine.fileInfo == null) {
                if (!"".equals(listLine.marker)) {
                    String markerJson = new String(UrlSafeBase64.decode(listLine.marker));
                    String key = JsonConvertUtils.fromJson(markerJson, JsonObject.class).get("k").getAsString();
                    return this.fileInfo.key.compareTo(key);
                }
                return -1;
            } else {
                return this.fileInfo.key.compareTo(listLine.fileInfo.key);
            }
        }

        public ListLine fromLine(String line) {

            if (!StringUtils.isNullOrEmpty(line)) {
                JsonObject json = new JsonObject();
                JsonElement item = json.get("item");
                JsonElement marker = json.get("marker");
                JsonElement dir = json.get("dir");
                if (item != null && !(item instanceof JsonNull)) {
                    this.fileInfo = JsonConvertUtils.fromJson(item, FileInfo.class);
                }
                if (marker != null && !(marker instanceof JsonNull)) {
                    this.marker = marker.getAsString();
                }
                if (dir != null && !(dir instanceof JsonNull)) {
                    this.dir = dir.getAsString();
                }
            }
            return this;
        }
    }
}
