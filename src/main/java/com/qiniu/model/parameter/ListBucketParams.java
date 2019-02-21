package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;
import java.util.*;

public class ListBucketParams extends QossParams {

    private String prefixes;
    private String antiPrefixes;
    private String prefixLeft;
    private String prefixRight;
    private String marker;
    private String end;

    public ListBucketParams(IEntryParam entryParam) throws IOException {
        super(entryParam);
        try { this.prefixes = entryParam.getParamValue("prefixes"); } catch (Exception e) { this.prefixes = ""; }
        try { this.antiPrefixes = entryParam.getParamValue("anti-prefixes"); } catch (Exception e) { this.antiPrefixes = ""; }
        try { this.prefixLeft = entryParam.getParamValue("prefix-left"); } catch (Exception e) { this.prefixLeft = ""; }
        try { this.prefixRight = entryParam.getParamValue("prefix-right"); } catch (Exception e) { this.prefixRight = ""; }
        try { this.marker = entryParam.getParamValue("marker"); } catch (Exception e) {}
        try { this.end = entryParam.getParamValue("end"); } catch (Exception e) {}
    }

    public List<String> getPrefixes() {
        if (!"".equals(prefixes)) {
            Set<String> set = new HashSet<>(Arrays.asList(prefixes.split(",")));
            return new ArrayList<>(set);
        }
        return null;
    }

    public List<String> getAntiPrefixes() {
        if (!"".equals(antiPrefixes)) {
            Set<String> set = new HashSet<>(Arrays.asList(antiPrefixes.split(",")));
            return new ArrayList<>(set);
        }
        return null;
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

    public String getMarker() {
        return marker;
    }

    public String getEnd() {
        return end;
    }
}
