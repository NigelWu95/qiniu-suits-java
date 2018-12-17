package com.qiniu.model.parameter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ListBucketParams extends QossParams {

    private String multiStatus;
    private String version;
    private String level;
    private String customPrefix;
    private String marker;
    private String end;
    private String antiPrefix;

    public ListBucketParams(String[] args) throws Exception {
        super(args);
        try { this.multiStatus = getParamFromArgs("multi"); } catch (Exception e) { multiStatus = ""; }
        try { this.version = getParamFromArgs("version"); } catch (Exception e) { version = ""; }
        try { this.level = getParamFromArgs("level"); } catch (Exception e) { level = ""; }
        try { this.customPrefix = getParamFromArgs("prefix"); } catch (Exception e) {}
        try { this.marker = getParamFromArgs("marker"); } catch (Exception e) {}
        try { this.end = getParamFromArgs("end"); } catch (Exception e) {}
        try { this.antiPrefix = getParamFromArgs("anti-prefix"); } catch (Exception e) { this.antiPrefix = ""; }
    }

    public ListBucketParams(String configFileName) throws Exception {
        super(configFileName);
        try { this.multiStatus = getParamFromConfig("multi"); } catch (Exception e) { multiStatus = "";}
        try { this.version = getParamFromConfig("version"); } catch (Exception e) { version = ""; }
        try { this.level = getParamFromConfig("level"); } catch (Exception e) { level = ""; }
        try { this.customPrefix = getParamFromConfig("prefix"); } catch (Exception e) {}
        try { this.marker = getParamFromConfig("marker"); } catch (Exception e) {}
        try { this.end = getParamFromConfig("end"); } catch (Exception e) {}
        try { this.antiPrefix = getParamFromConfig("anti-prefix"); } catch (Exception e) { this.antiPrefix = ""; }
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

    public int getLevel() {
        if (level.matches("[12]")) {
            return Integer.valueOf(level);
        } else {
            System.out.println("no incorrect level, it will use 1 as default.");
            return 1;
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
