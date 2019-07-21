package com.qiniu.datasource;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.qiniu.common.SuitsException;
import com.qiniu.sdk.FileItem;
import com.qiniu.sdk.UpYunClient;
import com.qiniu.util.JsonUtils;
import com.qiniu.util.ListingUtils;

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
        if (result.length() == 0) {
            this.marker = null;
            return fileItems;
        }
        JsonObject returnJson = JsonUtils.toJsonObject(result);
        this.marker = returnJson.has("iter") ? returnJson.get("iter").getAsString() : null;
        if ("g2gCZAAEbmV4dGQAA2VvZg".equals(this.marker)) this.marker = null;
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
                        directories.add(totalName);
                    } else {
                        directories.add(totalName);
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
        return fileItems;
    }

    private void checkedListWithEnd() {
        String endKey = currentEndKey();
        if (endPrefix == null || "".equals(endPrefix) || endKey == null) return;
        if (endKey.compareTo(endPrefix) == 0) {
            marker = null;
            if (endPrefix.equals(prefix + CloudStorageContainer.startPoint)) {
                FileItem last = currentLast();
                if (last != null && endPrefix.equals(last.key))
                    fileItems.remove(last);
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
    public synchronized List<FileItem> currents() {
        return fileItems;
    }

    public List<String> getDirectories() {
        return directories;
    }

    @Override
    public FileItem currentLast() {
        return fileItems.size() > 0 ? fileItems.get(fileItems.size() - 1) : null;
    }

    @Override
    public String currentEndKey() {
        if (hasNext()) return ListingUtils.decodeUpYunMarker(marker);
        FileItem last = currentLast();
        return last != null ? last.key : null;
    }

    @Override
    public void updateMarkerBy(FileItem object) {
        if (object != null) {
            marker = ListingUtils.getUpYunMarker(bucket, object);
        }
    }

    @Override
    public synchronized String truncate() {
        String endKey = currentEndKey();
        setEndPrefix(endKey);
        return endKey;
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
