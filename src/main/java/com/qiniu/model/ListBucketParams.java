package com.qiniu.model;

import com.qiniu.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ListBucketParams extends CommonParams {

    private String multiStatus;
    private String version;
    private String level;
    private String unitLen;
    private String customPrefix;
    private String antiPrefix;

    public ListBucketParams(String[] args) throws Exception {
        super(args);
        try { this.multiStatus = getParamFromArgs("multi"); } catch (Exception e) {}
        try { this.version = getParamFromArgs("version"); } catch (Exception e) {}
        try { this.level = getParamFromArgs("level"); } catch (Exception e) {}
        try { this.unitLen = getParamFromArgs("unit-len"); } catch (Exception e) {}
        try { this.customPrefix = getParamFromArgs("prefix"); } catch (Exception e) {}
        try { this.antiPrefix = getParamFromArgs("anti-prefix"); } catch (Exception e) { this.antiPrefix = ""; }
    }

    public ListBucketParams(String configFileName) throws Exception {
        super(configFileName);
        try { this.multiStatus = getParamFromConfig("multi"); } catch (Exception e) {}
        try { this.version = getParamFromConfig("version"); } catch (Exception e) {}
        try { this.level = getParamFromConfig("level"); } catch (Exception e) {}
        try { this.unitLen = getParamFromConfig("unit-len"); } catch (Exception e) {}
        try { this.customPrefix = getParamFromConfig("prefix"); } catch (Exception e) {}
        try { this.antiPrefix = getParamFromConfig("anti-prefix"); } catch (Exception e) { this.antiPrefix = ""; }
    }

    public boolean getMultiStatus() {
        if (StringUtils.isNullOrEmpty(multiStatus) || !multiStatus.matches("(true|false)")) {
            System.out.println("no incorrect multi status, it will use true as default.");
            return true;
        } else {
            return Boolean.valueOf(multiStatus);
        }
    }

    public int getVersion() {
        if (StringUtils.isNullOrEmpty(version) || !version.matches("[12]")) {
            System.out.println("no incorrect version, it will use 2 as default.");
            return 2;
        } else {
            return Integer.valueOf(version);
        }
    }

    public int getLevel() {
        if (StringUtils.isNullOrEmpty(level) || !level.matches("[12]")) {
            System.out.println("no incorrect level, it will use 1 as default.");
            return 1;
        } else {
            return Integer.valueOf(level);
        }
    }

    public int getUnitLen() {
        if (StringUtils.isNullOrEmpty(unitLen) || !unitLen.matches("\\d+")) {
            System.out.println("no incorrect unit-len, it will use 1000 as default.");
            return 1000;
        } else {
            return Integer.valueOf(unitLen);
        }
    }

    public String getCustomPrefix() {
        return customPrefix;
    }

    public List<String> getAntiPrefix() {
        if (StringUtils.isNullOrEmpty(antiPrefix)) return new ArrayList<>();
        return Arrays.asList(antiPrefix.split(","));
    }
}
