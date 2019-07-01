package com.qiniu.datasource;

import com.google.gson.*;
import com.qiniu.common.QiniuException;
import com.qiniu.common.SuitsException;
import com.qiniu.http.Response;
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
    private boolean straight;
    private List<FileInfo> fileInfoList;

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
    public void setPrefix(String prefix) {
        this.prefix = prefix;
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

    @Override
    public void setStraight(boolean straight) {
        this.straight = straight;
    }

    @Override
    public boolean canStraight() {
        return straight || !hasNext() || (endPrefix != null && !"".equals(endPrefix));
    }

    public List<FileInfo> getListResult(String prefix, String marker, int limit) throws IOException {
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
        String endKey = currentEndKey();
        // 删除大于 endPrefix 的元素，如果 endKey 大于等于 endPrefix 则需要进行筛选且使得 marker = null
        if (endPrefix == null || "".equals(endPrefix) || endKey == null) return;
        if (endKey.compareTo(endPrefix) == 0) {
            marker = null;
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
            throw new SuitsException(e.code(), LogUtils.getMessage(e));
        } catch (NullPointerException e) {
            throw new SuitsException(400000, "lister maybe already closed, " + e.getMessage());
        } catch (Exception e) {
            throw new SuitsException(-1, "failed, " + e.getMessage());
        }
    }

    @Override
    public void listForward() throws SuitsException {
        if (hasNext()) {
            doList();
        } else {
            fileInfoList.clear();
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
    public FileInfo currentLast() {
        return fileInfoList.size() > 0 ? fileInfoList.get(fileInfoList.size() - 1) : null;
    }

    @Override
    public String currentEndKey() {
        if (hasNext()) return OssUtils.decodeQiniuMarker(marker);
        FileInfo last = currentLast();
        return last != null ? last.key : null;
    }

    @Override
    public void updateMarkerBy(FileInfo object) {
        if (object != null) marker = OssUtils.getQiniuMarker(object.key);
    }

    @Override
    public void close() {
        bucketManager = null;
        fileInfoList = null;
    }
}
