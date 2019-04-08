package com.qiniu.datasource;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.qiniu.common.QiniuException;
import com.qiniu.common.SuitsException;
import com.qiniu.http.Response;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.JsonConvertUtils;
import com.qiniu.util.LogUtils;
import com.qiniu.util.StringUtils;
import com.qiniu.util.UrlSafeBase64;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class QiniuLister implements ILister<FileInfo> {

    private BucketManager bucketManager;
    private String bucket;
    private String prefix;
    private String marker;
    private String endPrefix;
    private String delimiter;
    private int limit;
    private List<FileInfo> fileInfoList;

    public QiniuLister(BucketManager bucketManager, String bucket, String prefix, String marker, String endPrefix,
                       String delimiter, int limit) throws IOException {
        this.bucketManager = bucketManager;
        this.bucket = bucket;
        this.prefix = prefix;
        this.marker = marker;
        this.endPrefix = endPrefix == null ? "" : endPrefix; // 初始值不使用 null，后续设置时可为空，便于判断是否进行过修改
        this.delimiter = delimiter;
        this.limit = limit;
        listForward();
    }

    @Override
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public String getPrefix() {
        return prefix;
    }

    @Override
    public void setMarker(String marker) {
        this.marker = marker;
    }

    @Override
    public String getMarker() {
        return marker;
    }

    @Override
    public void setEndPrefix(String endPrefix) {
        this.endPrefix = endPrefix;
    }

    @Override
    public String getEndPrefix() {
        return endPrefix;
    }

    @Override
    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public String getDelimiter() {
        return delimiter;
    }

    @Override
    public void setLimit(int limit) {
        this.limit = limit;
    }

    @Override
    public int getLimit() {
        return limit;
    }

    private List<FileInfo> getListResult(String prefix, String delimiter, String marker, int limit) throws IOException {
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
        bufferedReader.close();
        reader.close();
        inputStream.close();
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

    @Override
    public void listForward() throws SuitsException {
        try {
            List<FileInfo> current;
            do {
                current = getListResult(prefix, delimiter, marker, limit);
            } while (current.size() == 0 && hasNext());

            if (endPrefix != null && !"".equals(endPrefix)) {
                fileInfoList = current.stream()
                        .filter(fileInfo -> fileInfo.key.compareTo(endPrefix) < 0)
                        .collect(Collectors.toList());
                if (fileInfoList.size() < current.size()) marker = null;
            }
        } catch (QiniuException e) {
            throw new SuitsException(e.code(), LogUtils.getMessage(e));
        } catch (Exception e) {
            throw new SuitsException(-1, "failed, " + e.getMessage());
        }
    }

    @Override
    public boolean hasNext() {
        return marker != null && !"".equals(marker);
    }

    @Override
    public List<FileInfo> currents() {
        return fileInfoList;
    }

    @Override
    public FileInfo currentFirst() {
        return fileInfoList.size() > 0 ? fileInfoList.get(0) : null;
    }

    @Override
    public FileInfo currentLast() {
        return fileInfoList.size() > 0 ? fileInfoList.get(fileInfoList.size() - 1) : null;
    }

    @Override
    public void close() {
        bucketManager = null;
        fileInfoList = null;
    }

    public class ListLine implements Comparable {

        public FileInfo fileInfo;
        public String dir = "";
        public String marker = "";

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
                if (line != null && !"".equals(line)) {
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
