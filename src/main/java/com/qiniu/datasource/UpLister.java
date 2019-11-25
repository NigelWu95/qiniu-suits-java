package com.qiniu.datasource;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.qiniu.common.SuitsException;
import com.qiniu.interfaces.IPrefixLister;
import com.qiniu.sdk.FileItem;
import com.qiniu.sdk.UpYunClient;
import com.qiniu.util.JsonUtils;
import com.qiniu.util.CloudApiUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class UpLister implements IPrefixLister<FileItem> {

    private UpYunClient upYunClient;
    private final String bucket;
    private final String prefix;
    private String marker;
    private String endPrefix;
    private int limit;
    private String truncateMarker;
    private List<FileItem> fileItems;
    private FileItem last;
    private List<String> directories;
    private long count;

    public UpLister(UpYunClient upYunClient, String bucket, String prefix, String marker, String endPrefix,
                    int limit) throws SuitsException {
        this.upYunClient = upYunClient;
        this.bucket = bucket;
        this.prefix = prefix;
        this.marker = marker;
        this.endPrefix = endPrefix;
        this.limit = limit;
        doList();
        count += fileItems.size();
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
        count -= fileItems.size();
        checkedListWithEnd();
        count += fileItems.size();
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
        String result = upYunClient.listFiles(bucket, prefix, marker, limit);
        if (result == null || result.isEmpty()) {
            this.marker = null;
            return new ArrayList<>();
        }
        JsonObject returnJson = JsonUtils.toJsonObject(result);
        this.marker = returnJson.has("iter") ? returnJson.get("iter").getAsString() : null;
        if ("g2gCZAAEbmV4dGQAA2VvZg".equals(this.marker)) this.marker = null;
        JsonElement jsonElement = returnJson.get("files");
        if (jsonElement instanceof JsonArray) {
            JsonArray files = returnJson.get("files").getAsJsonArray();
            List<FileItem> fileItems = new ArrayList<>(files.size());
            if (files.size() > 0) {
                JsonObject object;
                String attribute;
                String totalName;
                for (JsonElement item : files) {
                    if (item == null || item instanceof JsonNull) continue;
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
                        fileItem.lastModified = object.get("last_modified").getAsLong();
                        fileItems.add(fileItem);
                    }
                }
            }
            return fileItems;
        } else {
            return new ArrayList<>();
        }
    }

    private void checkedListWithEnd() {
        if (endPrefix == null || "".equals(endPrefix)) return;
        String endKey = currentEndKey();
        if (endKey == null) return;
        if (endKey.compareTo(endPrefix) == 0) {
            marker = null;
            if (endPrefix.equals(prefix + CloudStorageContainer.firstPoint) && fileItems.size() > 0) {
                int lastIndex = fileItems.size() - 1;
                if (endPrefix.equals(fileItems.get(lastIndex).key)) fileItems.remove(lastIndex);
            }
        } else if (endKey.compareTo(endPrefix) > 0) {
            marker = null;
            int size = fileItems.size();
            // SDK 中返回的是 ArrayList，使用 remove 操作性能一般较差，同时也为了避免 Collectors.toList() 的频繁 new 操作，根据返
            // 回的 list 为文件名有序的特性，直接从 end 的位置进行截断
            int i = 0;
            for (; i < size; i++) {
                if (fileItems.get(i).key.compareTo(endPrefix) > 0) {
//                    fileItems.remove(i);
                    break;
                }
            }
            // 优化 gc，不用的元素全部清除
            fileItems.subList(i, size).clear();
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
            fileItems.clear();
            doList();
            count += fileItems.size();
        } else {
            if (fileItems.size() > 0) {
                last = fileItems.get(fileItems.size() - 1);
                fileItems.clear();
            }
        }
    }

    @Override
    public boolean hasNext() {
        return marker != null && !"".equals(marker) && !"g2gCZAAEbmV4dGQAA2VvZg".equals(marker);
    }

    @Override
    public boolean hasFutureNext() throws SuitsException {
        int expected = limit + 1;
        if (expected <= 10000) expected = 10001;
        int times = 10;
        List<FileItem> futureList = CloudApiUtils.initFutureList(limit, times);
        futureList.addAll(fileItems);
        fileItems.clear();
        SuitsException exception = null;
        while (futureList.size() < expected && times > 0 && hasNext()) {
            times--;
            try {
                doList();
                count += fileItems.size();
                futureList.addAll(fileItems);
                fileItems.clear();
            } catch (SuitsException e) {
//                fileItems = futureList;
//                throw e;
                exception = e;
            }
        }
        fileItems = futureList;
        futureList = null;
        if (exception != null) throw exception;
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
    public synchronized String currentEndKey() {
        if (hasNext()) return CloudApiUtils.decodeUpYunMarker(marker);
        if (truncateMarker != null && !"".equals(truncateMarker) && !"g2gCZAAEbmV4dGQAA2VvZg".equals(marker)) {
            return CloudApiUtils.decodeUpYunMarker(truncateMarker);
        }
        if (last != null) return last.key;
        if (fileItems.size() > 0) last = fileItems.get(fileItems.size() - 1);
        if (last != null) return last.key;
        return null;
    }

    @Override
    public synchronized String truncate() {
        truncateMarker = marker;
        marker = null;
        return truncateMarker;
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public void close() {
        upYunClient = null;
        endPrefix = null;
        if (fileItems.size() > 0) {
            last = fileItems.get(fileItems.size() - 1);
            fileItems.clear();
        }
//        directories = null;
    }
}
