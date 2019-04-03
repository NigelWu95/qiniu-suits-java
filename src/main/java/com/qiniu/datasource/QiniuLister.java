package com.qiniu.datasource;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.JsonConvertUtils;
import com.qiniu.util.LogUtils;
import com.qiniu.util.StringUtils;
import com.qiniu.util.UrlSafeBase64;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class QiniuLister implements ILister<List<FileInfo>, FileInfo> {

    private BucketManager bucketManager;
    private String bucket;
    private String prefix;
    private String marker;
    private String endKeyPrefix;
    private String delimiter;
    private int limit;
    private List<FileInfo> fileInfoList;
    private int statusCode;
    private String error;

    public QiniuLister(BucketManager bucketManager, String bucket, String prefix, String marker, String endKeyPrefix,
                       String delimiter, int limit) throws QiniuException {
        this.bucketManager = bucketManager;
        this.bucket = bucket;
        this.prefix = prefix;
        this.marker = marker;
        this.endKeyPrefix = endKeyPrefix == null ? "" : endKeyPrefix; // 初始值不使用 null，后续设置时可为空，便于判断是否进行过修改
        this.delimiter = delimiter;
        this.limit = limit;
        this.fileInfoList = getListResult(prefix, delimiter, marker, limit);
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getError() {
        return error;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getMarker() {
        return marker;
    }

    public void setMarker(String marker) {
        this.marker = marker;
    }

    public String getEndKeyPrefix() {
        return endKeyPrefix;
    }

    @Override
    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public void setEndKeyPrefix(String endKeyPrefix) {
        this.endKeyPrefix = endKeyPrefix;
    }

    public String getDelimiter() {
        return delimiter;
    }

    @Override
    public void setLimit(int limit) {
        this.limit = limit;
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
        List<ListLine> listLines = lines.stream()
                .map(line -> new ListLine().fromLine(line))
                .filter(Objects::nonNull)
                .sorted(ListLine::compareTo)
                .collect(Collectors.toList());
        response.close();
        // 转换成 ListLine 过程中可能出现问题，直接返回空列表，marker 不做修改，返回后则会再次使用同样的 marker 值进行列举
        if (listLines.size() < lines.size()) return new ArrayList<>();
        List<FileInfo> resultList = listLines.stream()
                .map(listLine -> listLine.fileInfo)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        this.marker = listLines.size() > 0 ? listLines.get(listLines.size() - 1).marker : null;
        return resultList;
    }

    public boolean checkMarkerValid() {
        return marker != null && !"".equals(marker);
    }

    public boolean checkListValid() {
        return fileInfoList != null && fileInfoList.size() > 0;
    }

    @Override
    public boolean hasNext() {
        return checkMarkerValid() || checkListValid();
    }

    @Override
    public List<FileInfo> next() throws IOException {
        List<FileInfo> current = fileInfoList == null ? new ArrayList<>() : fileInfoList;
        if (endKeyPrefix != null && !"".equals(endKeyPrefix)) {
            int size = current.size();
            current = current.stream()
                    .filter(fileInfo -> fileInfo.key.compareTo(endKeyPrefix) < 0)
                    .collect(Collectors.toList());
            int finalSize = current.size();
            if (finalSize < size) marker = null;
        }
        try {
            if (!checkMarkerValid()) fileInfoList = null;
            else {
                do {
                    fileInfoList = getListResult(prefix, delimiter, marker, limit);
                } while (!checkListValid() && checkMarkerValid());
            }
            statusCode = 200;
            error = null;
        } catch (QiniuException e) {
            fileInfoList = null;
            statusCode = e.code();
            error = LogUtils.getMessage(e);
            e.response.close();
        } catch (Exception e) {
            fileInfoList = null;
            statusCode = -1;
            error = "failed, " + e.getMessage();
        }
        return current;
    }

    @Override
    public FileInfo firstInNext() {
        return null;
    }

    @Override
    public FileInfo lastInNext() {
        return null;
    }

    @Override
    public void close() {
        this.bucketManager = null;
        this.fileInfoList = null;
    }

    public class ListLine implements Comparable {

        public FileInfo fileInfo;
        public String marker = "";
        public String dir = "";

        public boolean isDeleted() {
            return (fileInfo == null && (dir == null || "".equals(dir)));
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
