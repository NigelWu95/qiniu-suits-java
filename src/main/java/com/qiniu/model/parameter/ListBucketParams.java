package com.qiniu.model.parameter;

import com.google.gson.JsonObject;
import com.qiniu.config.JsonFile;
import com.qiniu.service.interfaces.IEntryParam;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.ListBucketUtils;

import java.io.IOException;
import java.util.*;

public class ListBucketParams extends CommonParams {

    private String accessKey;
    private String secretKey;
    private String bucket;
    private Map<String, String[]> prefixMap;
    private List<String> antiPrefixes;
    private boolean prefixLeft;
    private boolean prefixRight;

    public ListBucketParams(IEntryParam entryParam) throws IOException {
        super(entryParam);
        accessKey = entryParam.getValue("ak");
        secretKey = entryParam.getValue("sk");
        bucket = entryParam.getValue("bucket");
        setPrefixConfig(entryParam.getValue("prefix-config", ""), entryParam.getValue("prefixes", ""));
        setAntiPrefixes(entryParam.getValue("anti-prefixes", ""));
        setPrefixLeft(entryParam.getValue("prefix-left", ""));
        setPrefixRight(entryParam.getValue("prefix-right", ""));
    }

    private List<String> splitItems(String paramLine) {
        if (!"".equals(paramLine)) {
            Set<String> set;
            // 指定前缀包含 "," 号时需要用转义符解决
            if (paramLine.contains("\\,")) {
                String[] elements = paramLine.split("\\\\,");
                set = new HashSet<>(Arrays.asList(elements[0].split(",")));
                set.add(",");
                if (elements.length > 1)set.addAll(Arrays.asList(elements[1].split(",")));
            } else {
                set = new HashSet<>(Arrays.asList(paramLine.split(",")));
            }
            // 删除空前缀的情况避免列举操作时造成误解
            set.remove("");
            return new ArrayList<>(set);
        }
        return new ArrayList<>();
    }

    public void setAntiPrefixes(String antiPrefixes) {
        this.antiPrefixes = splitItems(antiPrefixes);
    }

    public void setPrefixLeft(String prefixLeft) {
        if (prefixLeft.matches("(true|false)")) {
            this.prefixLeft = Boolean.valueOf(prefixLeft);
        } else {
            this.prefixLeft = false;
        }
    }

    public void setPrefixRight(String prefixRight) {
        if (prefixRight.matches("(true|false)")) {
            this.prefixRight = Boolean.valueOf(prefixRight);
        } else {
            this.prefixRight = false;
        }
    }

    private String getMarker(String start, String marker, BucketManager bucketManager) throws IOException {
        if (!"".equals(marker) || "".equals(start)) return marker;
        else {
            FileInfo markerFileInfo = bucketManager.stat(bucket, start);
            markerFileInfo.key = start;
            return ListBucketUtils.calcMarker(markerFileInfo);
        }
    }

    public void setPrefixConfig(String prefixConfig, String prefixes) throws IOException {
        this.prefixMap = new HashMap<>();
        if (!"".equals(prefixConfig)) {
            JsonFile jsonFile = new JsonFile(prefixConfig);
            JsonObject jsonCfg;
            String marker;
            String end;
            BucketManager manager = new BucketManager(Auth.create(accessKey, secretKey), new Configuration());
            for (String prefix : jsonFile.getJsonObject().keySet()) {
                jsonCfg = jsonFile.getElement(prefix).getAsJsonObject();
                marker = getMarker(jsonCfg.get("start").getAsString(), jsonCfg.get("marker").getAsString(), manager);
                end = jsonCfg.get("end").getAsString();
                this.prefixMap.put(prefix, new String[]{marker, end});
            }
        } else {
            List<String> prefixList = splitItems(prefixes);
            for (String prefix : prefixList) {
                this.prefixMap.put(prefix, new String[]{"", ""});
            }
        }
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getBucket() {
        return bucket;
    }

    public List<String> getAntiPrefixes() {
        return antiPrefixes;
    }

    public boolean getPrefixLeft() {
        return prefixLeft;
    }

    public boolean getPrefixRight() {
        return prefixRight;
    }

    public Map<String, String[]> getPrefixMap() {
        return prefixMap;
    }
}
