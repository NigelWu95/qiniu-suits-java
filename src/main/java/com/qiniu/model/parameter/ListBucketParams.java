package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ListBucketParams extends QossParams {

    private String prefixes;
    private String antiPrefixes;
    private String marker;
    private String end;

    public ListBucketParams(IEntryParam entryParam) throws IOException {
        super(entryParam);
        try { this.prefixes = entryParam.getParamValue("prefixes"); } catch (Exception e) { this.prefixes = ""; }
        try { this.antiPrefixes = entryParam.getParamValue("anti-prefixes"); } catch (Exception e) { this.antiPrefixes = ""; }
        try { this.marker = entryParam.getParamValue("marker"); } catch (Exception e) {}
        try { this.end = entryParam.getParamValue("end"); } catch (Exception e) {}
    }

    public List<String> getPrefixes() {
        if (!"".equals(prefixes)) return Arrays.asList(prefixes.split(","));
        return null;
    }

    public String getMarker() {
        return marker;
    }

    public String getEnd() {
        return end;
    }

    public List<String> getAntiPrefixes() {
        if (!"".equals(antiPrefixes)) return Arrays.asList(antiPrefixes.split(","));
        return null;
    }
}
