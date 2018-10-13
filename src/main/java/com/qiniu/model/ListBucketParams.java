package com.qiniu.model;

import com.qiniu.util.StringUtils;

public class ListBucketParams extends BaseParams {

    private String maxThreads;
    private String version;
    private String enabledEndFile;
    private String level;
    private String process;
    private String processBatch;
    private String unitLen;

    public ListBucketParams(String[] args) throws Exception {
        super(args);
        try { this.maxThreads = getParamFromArgs("max-threads"); } catch (Exception e) {}
        try { this.version = getParamFromArgs("version"); } catch (Exception e) {}
        try { this.level = getParamFromArgs("level"); } catch (Exception e) {}
        try { this.process = getParamFromArgs("process"); } catch (Exception e) { this.process = ""; }
        try { this.processBatch = getParamFromArgs("process-batch"); } catch (Exception e) {}
        try { this.unitLen = getParamFromArgs("unit-len"); } catch (Exception e) {}
        try { this.enabledEndFile = getParamFromArgs("end-file"); } catch (Exception e) {}
    }

    public ListBucketParams(String configFileName) throws Exception {
        super(configFileName);
        try { this.maxThreads = getParamFromConfig("max-threads"); } catch (Exception e) {}
        try { this.version = getParamFromConfig("version"); } catch (Exception e) {}
        try { this.level = getParamFromConfig("level"); } catch (Exception e) {}
        try { this.process = getParamFromConfig("process"); } catch (Exception e) { this.process = ""; }
        try { this.processBatch = getParamFromConfig("process-batch"); } catch (Exception e) {}
        try { this.unitLen = getParamFromConfig("unit-len"); } catch (Exception e) {}
        try { this.enabledEndFile = getParamFromConfig("end-file"); } catch (Exception e) {}
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

    public int getUnitLen() {
        if (StringUtils.isNullOrEmpty(unitLen) || !unitLen.matches("\\d+")) {
            System.out.println("no incorrect unit-len, it will use 1000 as default.");
            return 1000;
        } else {
            return Integer.valueOf(unitLen);
        }
    }

    public boolean getEnabledEndFile() {
        if (StringUtils.isNullOrEmpty(enabledEndFile) || !enabledEndFile.matches("(true|false)")) {
            System.out.println("no incorrectly enable end-file, it will use false as default.");
            return false;
        } else {
            return Boolean.valueOf(enabledEndFile);
        }
    }
}