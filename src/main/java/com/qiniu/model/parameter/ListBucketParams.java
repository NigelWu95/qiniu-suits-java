package com.qiniu.model.parameter;

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
    private String marker;
    private String start;
    private String end;

    public ListBucketParams(IEntryParam entryParam) {
        super(entryParam);
        try { this.prefixes = entryParam.getParamValue("prefixes"); } catch (Exception e) { this.prefixes = ""; }
        try { this.antiPrefixes = entryParam.getParamValue("anti-prefixes"); } catch (Exception e) { this.antiPrefixes = ""; }
        try { this.prefixLeft = entryParam.getParamValue("prefix-left"); } catch (Exception e) { this.prefixLeft = ""; }
        try { this.prefixRight = entryParam.getParamValue("prefix-right"); } catch (Exception e) { this.prefixRight = ""; }
        try { this.marker = entryParam.getParamValue("marker"); } catch (Exception e) { this.marker = ""; }
        try { this.start = entryParam.getParamValue("start"); } catch (Exception e) { this.start = ""; }
        try { this.end = entryParam.getParamValue("end"); } catch (Exception e) {}
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

    public String getMarker() throws IOException {
        if (!"".equals(marker) || "".equals(start)) return marker;
        else {
            BucketManager bucketManager = new BucketManager(Auth.create(getAccessKey(), getSecretKey()),
                    new Configuration());
            FileInfo markerFileInfo = bucketManager.stat(getBucket(), start);
            markerFileInfo.key = start;
            return ListBucketUtils.calcMarker(markerFileInfo);
        }
    }

    public String getEnd() {
        return end;
    }
}
