package com.qiniu.datasource;

import com.google.gson.*;
import com.qiniu.common.QiniuException;
import com.qiniu.common.SuitsException;
import com.qiniu.http.Response;
import com.qiniu.interfaces.ILister;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class QiniuLister implements ILister<FileInfo> {

    private BucketManager bucketManager;
    private String bucket;
    private String prefix;
    private String marker;
    private String endPrefix;
    private int limit;
    private List<FileInfo> fileInfoList;
    private static final List<FileInfo> defaultList = new ArrayList<>();

    public QiniuLister(BucketManager bucketManager, String bucket, String prefix, String marker, String endPrefix,
                       int limit) throws SuitsException {
        this.bucketManager = bucketManager;
        this.bucket = bucket;
        this.prefix = prefix;
        this.marker = "".equals(marker) ? null : marker;
        this.endPrefix = endPrefix;
        this.limit = limit;
        doList();
    }

    @Override
    public String getBucket() {
        return bucket;
    }

    @Override
    public String getPrefix() {
        return prefix;
    }

    @Override
    public void setMarker(String marker) {
        this.marker = "".equals(marker) ? null : marker;
    }

    @Override
    public String getMarker() {
        return marker;
    }

    @Override
    public void setEndPrefix(String endPrefix) {
        this.endPrefix = endPrefix;
        checkedListWithEnd();
    }

    @Override
    public String getEndPrefix() {
        return endPrefix;
    }

    @Override
    public void setLimit(int limit) {
        this.limit = limit;
    }

    @Override
    public int getLimit() {
        return limit;
    }

    private List<FileInfo> getListResult(String prefix, String marker, int limit) throws IOException {
        Response response = bucketManager.listV2(bucket, prefix, marker, limit, null);
        if (response.statusCode != 200) throw new QiniuException(response);
        InputStream inputStream = new BufferedInputStream(response.bodyStream());
        Reader reader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(reader);
        List<FileInfo> fileInfoList = new ArrayList<>();
        JsonObject jsonObject = null;
        String line;
        try {
            while ((line = bufferedReader.readLine()) != null) {
                jsonObject = JsonUtils.toJsonObject(line);
                if (jsonObject.get("item") != null && !(jsonObject.get("item") instanceof JsonNull)) {
                    fileInfoList.add(JsonUtils.fromJson(jsonObject.get("item"), FileInfo.class));
                }
            }
            if (jsonObject != null && jsonObject.get("marker") != null && !(jsonObject.get("marker") instanceof JsonNull)) {
                this.marker = jsonObject.get("marker").getAsString();
            } else {
                this.marker = null;
            }
            return fileInfoList;
        } finally {
            try {
                bufferedReader.close();
                reader.close();
                inputStream.close();
                response.close();
            } catch (IOException e) {
                bufferedReader = null;
                reader = null;
                inputStream = null;
                response = null;
            }
        }
    }

    private void checkedListWithEnd() {
        if (endPrefix == null || "".equals(endPrefix)) return;
        String endKey = currentEndKey();
        // 删除大于 endPrefix 的元素，如果 endKey 大于等于 endPrefix 则需要进行筛选且使得 marker = null
        if (endKey == null) return;
        if (endKey.compareTo(endPrefix) == 0) {
            marker = null;
            // 由于 CloudStorageContainer 中设置 endPrefix 后下一级会从 endPrefix 开始直接列举，所以 endPrefix 这个文件名会出现重复，
            // 此处对其前者删除
            if (endPrefix.equals(prefix + CloudStorageContainer.firstPoint)) {
                if (fileInfoList.size() > 0) {
                    int lastIndex = fileInfoList.size() - 1;
                    FileInfo last = fileInfoList.get(lastIndex);
                    if (endPrefix.equals(last.key)) fileInfoList.remove(lastIndex);
                }
            }
        } else if (endKey.compareTo(endPrefix) > 0) {
            marker = null;
            int size = fileInfoList.size();
            // SDK 中返回的是 ArrayList，使用 remove 操作性能一般较差，同时也为了避免 Collectors.toList() 的频繁 new 操作，根据返
            // 回的 list 为文件名有序的特性，直接从 end 的位置进行截断
            for (int i = 0; i < size; i++) {
                if (fileInfoList.get(i).key.compareTo(endPrefix) > 0) {
                    fileInfoList = fileInfoList.subList(0, i);
                    return;
                }
            }
        }
    }

    private void doList() throws SuitsException {
        try {
            fileInfoList = getListResult(prefix, marker, limit);
            checkedListWithEnd();
        } catch (QiniuException e) {
            throw new SuitsException(e, e.code(), LogUtils.getMessage(e));
        } catch (NullPointerException e) {
            throw new SuitsException(e, 400000, "lister maybe already closed");
        } catch (Exception e) {
            throw new SuitsException(e, -1, "listing failed");
        }
    }

    @Override
    public synchronized void listForward() throws SuitsException {
        if (hasNext()) {
            doList();
        } else {
            fileInfoList = defaultList;
        }
    }

    @Override
    public boolean hasNext() {
        return marker != null && !"".equals(marker);
    }

    @Override
    public boolean hasFutureNext() throws SuitsException {
        int expected = limit + 1;
        if (expected <= 10000) expected = 10001;
        int times = 100000 / (fileInfoList.size() + 1) + 1;
        times = times > 10 ? 10 : times;
        List<FileInfo> futureList = fileInfoList;
        while (hasNext() && times > 0 && futureList.size() < expected) {
            // 优化大量删除情况下的列举速度，去掉 size>0 的条件
//            if (futureList.size() > 0)
                times--;
            doList();
            futureList.addAll(fileInfoList);
        }
        fileInfoList = futureList;
        return hasNext();
    }

    @Override
    public List<FileInfo> currents() {
        return fileInfoList;
    }

    @Override
    public String currentEndKey() {
        if (hasNext()) return CloudAPIUtils.decodeQiniuMarker(marker);
        if (fileInfoList.size() > 0) return fileInfoList.get(fileInfoList.size() - 1).key;
        return null;
    }

    @Override
    public synchronized String truncate() {
        String truncateMarker = marker;
        marker = null;
        return truncateMarker;
    }

    @Override
    public void close() {
        bucketManager = null;
        bucket = null;
        prefix = null;
        marker = null;
        endPrefix = null;
        fileInfoList = defaultList;
    }
}
