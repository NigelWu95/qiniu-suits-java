package com.qiniu.service.qoss;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.model.FileInfo;
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
    private List<FileInfo> fileInfoList;
    public QiniuException exception;

    public FileLister(BucketManager bucketManager, String bucket, String prefix, String delimiter, String marker,
                      int limit) throws QiniuException {
        this.bucketManager = bucketManager;
        this.bucket = bucket;
        this.prefix = prefix;
        this.delimiter = delimiter;
        this.marker = marker;
        this.limit = limit;
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

    private List<FileInfo> getListResult(String prefix, String delimiter, String marker, int limit) throws QiniuException {
        Response response = bucketManager.listV2(bucket, prefix, marker, limit, delimiter);
        InputStream inputStream = new BufferedInputStream(response.bodyStream());
        Reader reader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(reader);
        List<String> lines = bufferedReader.lines()
                    .filter(line -> !StringUtils.isNullOrEmpty(line))
                    .collect(Collectors.toList());
        List<ListLine> listLines = lines.parallelStream()
                .map(line -> new ListLine().fromLine(line))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (listLines.size() < lines.size()) {
            throw new QiniuException(new QiniuException(response), "convert line to file info error.");
        }
        List<FileInfo> resultList = listLines.parallelStream()
                .map(listLine -> listLine.fileInfo)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        Optional<ListLine> lastListLine = listLines.parallelStream()
                .max(ListLine::compareTo);
        this.marker = lastListLine.map(listLine -> listLine.marker).orElse("");
        response.close();
        return resultList;
    }

    private boolean checkMarkerValid() {
        return marker != null && !"".equals(marker);
    }

    private boolean checkListValid() {
        return fileInfoList != null && fileInfoList.size() > 0;
    }

    @Override
    public boolean hasNext() {
        return checkMarkerValid() || checkListValid();
    }

    @Override
    public List<FileInfo> next() {
        List<FileInfo> current = fileInfoList == null ? new ArrayList<>() : fileInfoList;
        try {
            if (!checkMarkerValid()) fileInfoList = null;
            else {
                do {
                    fileInfoList = getListResult(prefix, delimiter, marker, limit);
                } while (!checkListValid() && checkMarkerValid());
            }
        } catch (Exception e) {
            fileInfoList = null;
            exception = new QiniuException(e);
        }
        return current;
    }

    @Override
    public void remove() {
        this.bucketManager = null;
        this.fileInfoList = null;
        this.exception = null;
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
            try {
                if (!StringUtils.isNullOrEmpty(line)) {
                    JsonObject json = JsonConvertUtils.toJsonObject(line);
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
            } catch (Exception e) {
                return null;
            }

        }
    }
}
