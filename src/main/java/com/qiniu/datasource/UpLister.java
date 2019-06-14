package com.qiniu.datasource;

import com.qiniu.common.SuitsException;
import com.qiniu.sdk.FileItem;
import com.qiniu.sdk.UpYunClient;
import com.qiniu.util.OssUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;

public class UpLister implements ILister<FileItem> {

    private UpYunClient upYunClient;
    private String bucket;
    private String prefix;
    private String marker;
    private String endPrefix;
    private int limit;
    private boolean straight;
    private List<FileItem> fileItems;
    private List<String> directories = new ArrayList<>();

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
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public String getPrefix() {
        return prefix;
    }

    @Override
    public void setMarker(String marker) {
        this.marker = marker == null ? "" : marker;
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
    public void setDelimiter(String delimiter) {}

    @Override
    public String getDelimiter() {
        return null;
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
    public boolean getStraight() {
        return straight;
    }

    @Override
    public boolean canStraight() {
        return straight || !hasNext() || (endPrefix != null && !"".equals(endPrefix));
    }

    private List<FileItem> getListResult(String prefix, String marker, int limit) throws IOException {
        StringBuilder text = new StringBuilder();
        List<FileItem> fileItems = new ArrayList<>();
        HttpURLConnection conn = null;
        int code;
        InputStream is = null;
        InputStreamReader sr = null;
        BufferedReader br = null;
        try {
            conn = upYunClient.listFilesConnection(bucket, prefix, marker, limit);
            code = conn.getResponseCode();
//        is = conn.getInputStream(); // 状态码错误时不能使用 getInputStream()
            is = code >= 400 ? conn.getErrorStream() : conn.getInputStream();
            sr = new InputStreamReader(is);
            br = new BufferedReader(sr);
            char[] chars = new char[4096];
            int length;
            while ((length = br.read(chars)) != -1) {
                text.append(chars, 0, length);
            }
            this.marker = conn.getHeaderField("x-upyun-list-iter");
            if ("g2gCZAAEbmV4dGQAA2VvZg".equals(this.marker) || text.length() == 0) this.marker = null;
            if (code == 200) {
                String result = text.toString();
                String[] lines = result.split("\n");
                for (String line : lines) {
                    if (line.indexOf("\t") > 0) {
                        FileItem fileItem = new FileItem(line);
                        if ("N".equals(fileItem.attribute)) fileItems.add(fileItem);
                        else directories.add(fileItem.key);
                    }
                }
                return fileItems;
            } else if (code == 404) {
                this.marker = null;
                return fileItems;
            } else {
                throw new SuitsException(code, text.toString());
            }
        } finally {
            try {
                if (conn != null) conn.disconnect();
                if (br != null) br.close();
                if (sr != null) sr.close();
                if (is != null) is.close();
            } catch (IOException e) {
                br = null;
                sr = null;
                is = null;
            }
        }
    }

    private void checkedListWithEnd() {
        String endKey = currentEndKey();
        // 删除大于 endPrefix 的元素，如果 endKey 大于等于 endPrefix 则需要进行筛选且使得 marker = null
        if (endPrefix != null && !"".equals(endPrefix) && endKey != null && endKey.compareTo(endPrefix) >= 0) {
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
            fileItems.clear();
        }
    }

    @Override
    public boolean hasNext() {
        return marker != null && !"".equals(marker);
    }

    @Override
    public boolean hasFutureNext() throws SuitsException {
        int times = 50000 / (fileItems.size() + 1);
        times = times > 10 ? 10 : times;
        List<FileItem> futureList = fileItems;
        while (hasNext() && times > 0 && futureList.size() < 10001) {
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
    public FileItem currentLast() {
        return fileItems.size() > 0 ? fileItems.get(fileItems.size() - 1) : null;
    }

    @Override
    public String currentEndKey() {
        if (hasNext()) return OssUtils.decodeUpYunMarker(bucket, marker);
        FileItem last = currentLast();
        return last != null ? last.key : null;
    }

    @Override
    public void updateMarkerBy(FileItem object) {
        if (object != null) {
            marker = OssUtils.getUpYunMarker(bucket, object);
        }
    }

    @Override
    public void close() {
        upYunClient = null;
        fileItems = null;
    }
}
