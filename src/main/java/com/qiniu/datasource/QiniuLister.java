package com.qiniu.datasource;

import com.google.gson.*;
import com.qiniu.common.Constants;
import com.qiniu.common.QiniuException;
import com.qiniu.common.SuitsException;
import com.qiniu.http.Response;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.*;

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
    private boolean straight;
    private List<FileInfo> fileInfoList;

    public QiniuLister(BucketManager bucketManager, String bucket, String prefix, String marker, String endPrefix,
                       String delimiter, int limit) throws SuitsException {
        this.bucketManager = bucketManager;
        this.bucket = bucket;
        this.prefix = prefix;
        this.marker = marker;
        this.endPrefix = endPrefix;
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
        if (endPrefix != null && !"".equals(endPrefix)) {
            int size = fileInfoList.size();
            fileInfoList = fileInfoList.stream()
                    .filter(fileInfo -> fileInfo.key.compareTo(endPrefix) < 0)
                    .collect(Collectors.toList());
            if (fileInfoList.size() < size) marker = null;
        }
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

    @Override
    public void setStraight(boolean straight) {
        this.straight = straight;
    }

    @Override
    public boolean canStraight() {
        return straight || (endPrefix != null && !"".equals(endPrefix));
    }

    private List<JsonObject> getListResult(String prefix, String delimiter, String marker, int limit) throws QiniuException {
        Response response = bucketManager.listV2(bucket, prefix, marker, limit, delimiter);
        InputStream inputStream = new BufferedInputStream(response.bodyStream());
        Reader reader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(reader);
        List<JsonObject> jsonObjects = new ArrayList<>();
        try {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                jsonObjects.add(JsonConvertUtils.toJsonObject(line));
            }
            bufferedReader.close();
            reader.close();
            inputStream.close();
            response.close();
        } catch (IOException e) {
            throw new QiniuException(e, e.getMessage());
        }
        return jsonObjects;
    }

    private List<FileInfo> doList(String prefix, String delimiter, String marker, int limit) throws QiniuException {
        List<JsonObject> jsonObjects = getListResult(prefix, delimiter, marker, limit);
        JsonObject lastJson = jsonObjects.size() > 0 ? jsonObjects.get(jsonObjects.size() - 1) : null;
        try {
            if (lastJson != null && lastJson.get("marker") != null && !(lastJson.get("marker") instanceof JsonNull)) {
                this.marker = lastJson.get("marker").getAsString();
            } else {
                this.marker = null;
            }
            return jsonObjects.stream().map(jsonObject -> {
                if (jsonObject.get("item") != null && !(jsonObject.get("item") instanceof JsonNull)) {
                    return JsonConvertUtils.fromJson(jsonObject.get("item"), FileInfo.class);
                } else {
                    return null;
                }
            }).filter(Objects::nonNull).collect(Collectors.toList());
        } catch (Exception e) {
            throw new QiniuException(e, e.getMessage());
        }
    }

    @Override
    public void listForward() throws SuitsException {
        try {
            List<FileInfo> current;
            do {
                current = doList(prefix, delimiter, marker, limit);
            } while (current.size() == 0 && hasNext());

            if (endPrefix != null && !"".equals(endPrefix)) {
                fileInfoList = current.stream()
                        .filter(fileInfo -> fileInfo.key.compareTo(endPrefix) < 0)
                        .collect(Collectors.toList());
                if (fileInfoList.size() < current.size()) marker = null;
            } else {
                fileInfoList = current;
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
    public boolean hasFutureNext() throws SuitsException {
        String marker = this.marker;
        List<JsonObject> jsonObjects;
        JsonObject lastJson;
        int times = 10;
        while (times > 0) {
            try {
                jsonObjects = getListResult(prefix, delimiter, marker, limit);
                lastJson = jsonObjects.size() > 0 ? jsonObjects.get(jsonObjects.size() - 1) : null;
                if (lastJson != null && lastJson.get("marker") != null && !(lastJson.get("marker") instanceof JsonNull)) {
                    marker = lastJson.get("marker").getAsString();
                    if (marker == null || "".equals(marker)) return false;
                } else {
                    return false;
                }
                times--;
            } catch (Exception e) {
                if (e instanceof QiniuException && ((QiniuException) e).code() >= 500) {
                    throw new SuitsException(((QiniuException) e).code(), e.getMessage());
                } else {
                    e.printStackTrace();
                }
            }
        }
        return true;
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
    public String currentFirstKey() {
        FileInfo first = currentFirst();
        return first != null ? first.key : null;
    }

    @Override
    public FileInfo currentLast() {
        FileInfo last = fileInfoList.size() > 0 ? fileInfoList.get(fileInfoList.size() - 1) : null;
        if (last == null && marker != null) {
            String decodedMarker = new String(Base64.decode(marker, Base64.URL_SAFE | Base64.NO_WRAP));
            JsonObject jsonObject = new JsonParser().parse(decodedMarker).getAsJsonObject();
            last = new FileInfo();
            last.key = jsonObject.get("k").getAsString();
        }
        return last;
    }

    @Override
    public String currentLastKey() {
        FileInfo last = currentLast();
        return last != null ? last.key : null;
    }

    @Override
    public void updateMarkerBy(FileInfo object) {
        if (object != null) {
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("k", object.key);
            marker = Base64.encodeToString(JsonConvertUtils.toJson(jsonObject).getBytes(Constants.UTF_8),
                    Base64.URL_SAFE | Base64.NO_WRAP);
        } else {
            marker = null;
        }
    }

    @Override
    public void close() {
        bucketManager = null;
        fileInfoList = null;
    }
}
