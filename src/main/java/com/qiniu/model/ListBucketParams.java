package com.qiniu.model;

import com.qiniu.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ListBucketParams extends BaseParams {

    private String multiStatus;
    private String maxThreads;
    private String version;
    private String level;
    private String unitLen;
    private String customPrefix;
    private String antiPrefix;
    private String resultFormat;
    private String resultFileDir;
    private String process;
    private String processBatch;

    public ListBucketParams(String[] args) throws Exception {
        super(args);
        try { this.multiStatus = getParamFromArgs("multi"); } catch (Exception e) {}
        try { this.maxThreads = getParamFromArgs("max-threads"); } catch (Exception e) {}
        try { this.version = getParamFromArgs("version"); } catch (Exception e) {}
        try { this.level = getParamFromArgs("level"); } catch (Exception e) {}
        try { this.unitLen = getParamFromArgs("unit-len"); } catch (Exception e) {}
        try { this.customPrefix = getParamFromArgs("prefix"); } catch (Exception e) {}
        try { this.antiPrefix = getParamFromArgs("anti-prefix"); } catch (Exception e) { this.antiPrefix = ""; }
        try {
            this.resultFormat = getParamFromArgs("result-format");
        } catch (Exception e) {
            this.resultFormat = "json";
        }
        try {
            this.resultFileDir = getParamFromArgs("result-path");
        } catch (Exception e) {
            this.resultFileDir = "../result";
        }
        try { this.process = getParamFromArgs("process"); } catch (Exception e) { this.process = ""; }
        try { this.processBatch = getParamFromArgs("process-batch"); } catch (Exception e) {}
    }

    public ListBucketParams(String configFileName) throws Exception {
        super(configFileName);
        try { this.multiStatus = getParamFromConfig("multi"); } catch (Exception e) {}
        try { this.maxThreads = getParamFromConfig("max-threads"); } catch (Exception e) {}
        try { this.version = getParamFromConfig("version"); } catch (Exception e) {}
        try { this.level = getParamFromConfig("level"); } catch (Exception e) {}
        try { this.unitLen = getParamFromConfig("unit-len"); } catch (Exception e) {}
        try { this.customPrefix = getParamFromConfig("prefix"); } catch (Exception e) {}
        try { this.antiPrefix = getParamFromConfig("anti-prefix"); } catch (Exception e) { this.antiPrefix = ""; }
        try {
            this.resultFormat = getParamFromConfig("result-format");
        } catch (Exception e) {
            this.resultFormat = "json";
        }
        try {
            this.resultFileDir = getParamFromConfig("result-path");
        } catch (Exception e) {
            this.resultFileDir = "../result";
        }
        try { this.process = getParamFromConfig("process"); } catch (Exception e) { this.process = ""; }
        try { this.processBatch = getParamFromConfig("process-batch"); } catch (Exception e) {}
    }

    public boolean getMultiStatus() {
        if (StringUtils.isNullOrEmpty(multiStatus) || !multiStatus.matches("(true|false)")) {
            System.out.println("no incorrectly enable multi, it will use true as default.");
            return true;
        } else {
            return Boolean.valueOf(multiStatus);
        }
    }

    public int getMaxThreads() {
        if (StringUtils.isNullOrEmpty(unitLen) || !maxThreads.matches("[1-9]\\d*")) {
            System.out.println("no incorrect threads, it will use 10 as default.");
            return 10;
        } else {
            return Integer.valueOf(maxThreads);
        }
    }

    public int getVersion() {
        if (StringUtils.isNullOrEmpty(unitLen) || !version.matches("[12]")) {
            System.out.println("no incorrect version, it will use 2 as default.");
            return 2;
        } else {
            return Integer.valueOf(version);
        }
    }

    public int getLevel() {
        if (StringUtils.isNullOrEmpty(unitLen) || !level.matches("[12]")) {
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

    public String getResultFormat() {
        return resultFormat;
    }

    public String getResultFileDir() {
        return System.getProperty("user.dir") + System.getProperty("file.separator") + resultFileDir;
    }

    public String getProcess() {
        return process;
    }

    public boolean getProcessBatch() {
        if (StringUtils.isNullOrEmpty(processBatch) || !processBatch.matches("(true|false)")) {
            System.out.println("no incorrectly process-batch, it will use false as default.");
            return false;
        } else {
            return Boolean.valueOf(processBatch);
        }
    }
}