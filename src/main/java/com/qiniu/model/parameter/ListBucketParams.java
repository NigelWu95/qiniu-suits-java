package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ListBucketParams extends QossParams {

    private String multiStatus;
    private String version;
    private String customPrefix;
    private String marker;
    private String end;
    private String antiPrefix;

    public ListBucketParams(IEntryParam entryParam) {
        super(entryParam);
        try { this.multiStatus = entryParam.getParamValue("multi"); } catch (Exception e) { multiStatus = ""; }
        try { this.version = entryParam.getParamValue("version"); } catch (Exception e) { version = ""; }
        try { this.customPrefix = entryParam.getParamValue("prefix"); } catch (Exception e) {}
        try { this.marker = entryParam.getParamValue("marker"); } catch (Exception e) {}
        try { this.end = entryParam.getParamValue("end"); } catch (Exception e) {}
        try { this.antiPrefix = entryParam.getParamValue("anti-prefix"); } catch (Exception e) { this.antiPrefix = ""; }
    }

    public boolean getMultiStatus() {
        if (multiStatus.matches("(true|false)")) {
            return Boolean.valueOf(multiStatus);
        } else {
            System.out.println("no incorrect multi status, it will use true as default.");
            return true;
        }
    }

    public int getVersion() {
        if (version.matches("[12]")) {
            return Integer.valueOf(version);
        } else {
            System.out.println("no incorrect version, it will use 2 as default.");
            return 2;
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
        if (antiPrefix == null || "".equals(antiPrefix)) return new ArrayList<>();
        return Arrays.asList(antiPrefix.split(","));
    }

    public Boolean getSaveTotal() {
        if (saveTotal.matches("(true|false)")) {
            return Boolean.valueOf(saveTotal);
        } else {
            System.out.println("not incorrectly set result save total option, it will use \"true\" as default.");
            return true;
        }
    }
}
