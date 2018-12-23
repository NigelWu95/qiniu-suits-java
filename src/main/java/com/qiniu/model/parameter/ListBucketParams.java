package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ListBucketParams extends QossParams {

    private String multiStatus;
    private String customPrefix;
    private String marker;
    private String end;
    private String antiPrefix;

    public ListBucketParams(IEntryParam entryParam) {
        super(entryParam);
        try { this.multiStatus = entryParam.getParamValue("multi"); } catch (Exception e) { multiStatus = ""; }
        try { this.customPrefix = entryParam.getParamValue("prefix"); } catch (Exception e) {}
        try { this.marker = entryParam.getParamValue("marker"); } catch (Exception e) {}
        try { this.end = entryParam.getParamValue("end"); } catch (Exception e) {}
        try { this.antiPrefix = entryParam.getParamValue("anti-prefix"); } catch (Exception e) { this.antiPrefix = ""; }
    }

    public boolean getMultiStatus() {
        if (multiStatus.matches("(true|false)")) {
            return Boolean.valueOf(multiStatus);
        } else {
            return true;
        }
    }

    public String getCustomPrefix() {
        return customPrefix;
    }

    public String getMarker() {
        return marker;
    }

    public String getEnd() {
        return end;
    }

    public List<String> getAntiPrefix() {
        if (!"".equals(antiPrefix)) return Arrays.asList(antiPrefix.split(","));
        return null;
    }

    public Boolean getSaveTotal() {
        if (saveTotal.matches("(true|false)")) {
            return Boolean.valueOf(saveTotal);
        } else {
            return true;
        }
    }
}
