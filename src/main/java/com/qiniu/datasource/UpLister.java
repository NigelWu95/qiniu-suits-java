package com.qiniu.datasource;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.qiniu.common.SuitsException;
import com.qiniu.sdk.FileItem;
import com.qiniu.sdk.UpYunClient;
import com.qiniu.util.JsonUtils;
import com.qiniu.util.CloudAPIUtils;
import com.qiniu.util.URLUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class UpLister implements ILister<FileItem> {

    private UpYunClient upYunClient;
    private String bucket;
    private String prefix;
    private String marker;
    private String endPrefix;
    private int limit;
    private List<FileItem> fileItems;
    private List<String> directories;

    public UpLister(UpYunClient upYunClient, String bucket, String prefix, String marker, String endPrefix,
                    int limit) throws SuitsException {
        this.upYunClient = upYunClient;
        this.bucket = bucket;
        this.prefix = prefix;
        this.marker = marker;
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
        this.marker = marker;
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

    private List<FileItem> getListResult(String prefix, String marker, int limit) throws IOException {
        List<FileItem> fileItems = new ArrayList<>();
        String result = upYunClient.listFiles(bucket, prefix, marker, limit);
        if (result == null || result.isEmpty()) {
            this.marker = null;
            return fileItems;
        }
        JsonObject returnJson = JsonUtils.toJsonObject(result);
        this.marker = returnJson.has("iter") ? returnJson.get("iter").getAsString() : null;
        if ("g2gCZAAEbmV4dGQAA2VvZg".equals(this.marker)) this.marker = null;
        JsonElement jsonElement = returnJson.get("files");
        if (jsonElement instanceof JsonArray) {
            JsonArray files = returnJson.get("files").getAsJsonArray();
            if (files.size() > 0) {
                JsonObject object;
                String attribute;
                String totalName;
                for (JsonElement item : files) {
                    object = item.getAsJsonObject();
                    attribute = object.get("type").getAsString();
                    totalName = prefix == null || prefix.isEmpty() ? object.get("name").getAsString() :
                            prefix + "/" + object.get("name").getAsString();
                    if ("folder".equals(attribute)) {
                        if (directories == null) {
                            directories = new ArrayList<>();
                            directories.add(URLUtils.getEncodedURI(totalName));
                        } else {
                            directories.add(URLUtils.getEncodedURI(totalName));
                        }
                    } else {
                        FileItem fileItem = new FileItem();
                        fileItem.key = totalName;
                        fileItem.attribute = attribute;
                        fileItem.size = object.get("length").getAsLong();
                        fileItem.timeSeconds = object.get("last_modified").getAsLong();
                        fileItems.add(fileItem);
                    }
                }
            }
        }
        return fileItems;
    }

    private void checkedListWithEnd() {
        if (endPrefix == null || "".equals(endPrefix)) return;
        String endKey = currentEndKey();
        if (endKey == null) return;
        if (endKey.compareTo(endPrefix) == 0) {
            marker = null;
            if (endPrefix.equals(prefix + CloudStorageContainer.firstPoint)) {
                if (fileItems.size() > 0) {
                    int lastIndex = fileItems.size() - 1;
                    FileItem last = fileItems.get(lastIndex);
                    if (endPrefix.equals(last.key)) fileItems.remove(lastIndex);
                }
            }
        } else if (endKey.compareTo(endPrefix) > 0) {
            marker = null;
            int size = fileItems.size();
            // SDK 中返回的是 ArrayList，使用 remove 操作性能一般较差，同时也为了避免 Collectors.toList() 的频繁 new 操作，根据返
            // 回的 list 为文件名有序的特性，直接从 end 的位置进行截断
            for (int i = 0; i < size; i++) {
                if (fileItems.get(i).key.compareTo(endPrefix) > 0) {
                    fileItems = fileItems.subList(0, i);
                    return;
                }
            }
        }
    }

    private void doList() throws SuitsException {
        try {
            fileItems = getListResult(prefix, marker, limit);
            checkedListWithEnd();
        } catch (SuitsException e) {
            throw e;
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
            fileItems.clear();
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
        int times = 100000 / (fileItems.size() + 1) + 1;
        times = times > 10 ? 10 : times;
        List<FileItem> futureList = fileItems;
        while (hasNext() && times > 0 && futureList.size() < expected) {
            times--;
            doList();
            futureList.addAll(fileItems);
        }
        fileItems = futureList;
        return hasNext();
    }

    @Override
    public List<FileItem> currents() {
        return fileItems;
    }

    public List<String> getDirectories() {
        return directories;
    }

    @Override
    public String currentEndKey() {
        if (hasNext()) return CloudAPIUtils.decodeUpYunMarker(marker);
        if (fileItems.size() > 0) return fileItems.get(fileItems.size() - 1).key;
        return null;
    }

    @Override
    public synchronized String truncate() {
        String truncateMarker = null;
        if (hasNext()) {
            truncateMarker = marker;
            marker = null;
        }
        return truncateMarker;
    }

    @Override
    public void close() {
        upYunClient = null;
        bucket = null;
        prefix = null;
        marker = null;
        endPrefix = null;
        fileItems.clear();
//        directories = null;
    }
}
