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

public class ListBucketParams extends QossParams {

    private String prefixes;
    private String antiPrefixes;
    private String prefixLeft;
    private String prefixRight;
    private String prefixConfig;

    public ListBucketParams(IEntryParam entryParam) {
        super(entryParam);
        try { prefixes = entryParam.getParamValue("prefixes"); } catch (Exception e) { prefixes = ""; }
        try { antiPrefixes = entryParam.getParamValue("anti-prefixes"); } catch (Exception e) { antiPrefixes = ""; }
        try { prefixLeft = entryParam.getParamValue("prefix-left"); } catch (Exception e) { prefixLeft = ""; }
        try { prefixRight = entryParam.getParamValue("prefix-right"); } catch (Exception e) { prefixRight = ""; }
        try { prefixConfig = entryParam.getParamValue("prefix-config"); } catch (Exception e) { prefixConfig = ""; }
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
        return null;
    }

    public List<String> getPrefixes() {
        return splitItems(prefixes);
    }

    public List<String> getAntiPrefixes() {
        return splitItems(antiPrefixes);
    }

    public boolean getPrefixLeft() {
        if (prefixLeft.matches("(true|false)")) {
            return Boolean.valueOf(prefixLeft);
        } else {
            return false;
        }
    }

    public boolean getPrefixRight() {
        if (prefixRight.matches("(true|false)")) {
            return Boolean.valueOf(prefixRight);
        } else {
            return false;
        }
    }

    public String getMarker(String start, String marker, BucketManager bucketManager) throws IOException {
        if (!"".equals(marker) || "".equals(start)) return marker;
        else {
            FileInfo markerFileInfo = bucketManager.stat(getBucket(), start);
            markerFileInfo.key = start;
            return ListBucketUtils.calcMarker(markerFileInfo);
        }
    }

    public Map<String, String[]> getPrefixConfig() throws IOException {
        Map<String, String[]> prefixes = new HashMap<>();
        if (!"".equals(prefixConfig)) {
            JsonFile jsonFile = new JsonFile(prefixConfig);
            JsonObject jsonCfg;
            String marker;
            String end;
            BucketManager manager = new BucketManager(Auth.create(getAccessKey(), getSecretKey()), new Configuration());
            for (String prefix : jsonFile.getJsonObject().keySet()) {
                jsonCfg = jsonFile.getElement(prefix).getAsJsonObject();
                marker = getMarker(jsonCfg.get("start").getAsString(), jsonCfg.get("marker").getAsString(), manager);
                end = jsonCfg.get("end").getAsString();
                prefixes.put(prefix, new String[]{marker, end});
            }
        } else {
            List<String> prefixList = getPrefixes();
            for (String prefix : prefixList) {
                prefixes.put(prefix, new String[]{"", ""});
            }
        }
        return prefixes;
    }
}
