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

    public ListBucketParams(IEntryParam entryParam) {
        super(entryParam);
        try { this.prefixes = entryParam.getParamValue("prefixes"); } catch (Exception e) { this.prefixes = ""; }
        try { this.antiPrefixes = entryParam.getParamValue("anti-prefixes"); } catch (Exception e) { this.antiPrefixes = ""; }
        try { this.prefixLeft = entryParam.getParamValue("prefix-left"); } catch (Exception e) { this.prefixLeft = ""; }
        try { this.prefixRight = entryParam.getParamValue("prefix-right"); } catch (Exception e) { this.prefixRight = ""; }
        try { this.marker = entryParam.getParamValue("marker"); } catch (Exception e) {}
        try { this.end = entryParam.getParamValue("end"); } catch (Exception e) {}
    }

    private List<String> splitItems(String paramLine) {
        if (!"".equals(paramLine)) {
            Set<String> set;
            if (paramLine.contains("\\,")) {
                String[] elements = paramLine.split("\\\\,");
                set = new HashSet<>(Arrays.asList(elements[0].split(",")));
                set.add(",");
                if (elements.length > 1)set.addAll(Arrays.asList(elements[1].split(",")));
            } else {
                set = new HashSet<>(Arrays.asList(paramLine.split(",")));
            }
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

    public String getMarker() {
        return marker;
    }

    public String getEnd() {
        return end;
    }
}
