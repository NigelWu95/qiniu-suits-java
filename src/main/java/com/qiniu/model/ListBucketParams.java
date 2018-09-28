package com.qiniu.model;

public class ListBucketParams extends BaseParams {

    private String maxThreads;
    private String version;
    private String withParallel;
    private String level;
    private String process;
    private String unitLen;

    public ListBucketParams(String[] args) throws Exception {
        super(args);
        this.maxThreads = getParamFromArgs("max-threads");
        this.version = getParamFromArgs("version");
        this.withParallel = getParamFromArgs("parallel");
        this.level = getParamFromArgs("level");
        try {
            this.process = getParamFromArgs("process");
        } catch (Exception e) {}
        this.unitLen = getParamFromArgs("unit-len");;
    }

    public ListBucketParams(String configFileName) throws Exception {
        super(configFileName);
        this.maxThreads = getParamFromConfig("max-threads");
        this.version = getParamFromConfig("version");
        this.withParallel = getParamFromConfig("parallel");
        this.level = getParamFromConfig("level");
        try {
            this.process = getParamFromConfig("process");
        } catch (Exception e) {}
        this.unitLen = getParamFromConfig("unit-len");;
    }

    public int getMaxThreads() {
        if (maxThreads.matches("[1-9]\\d*")) {
            return Integer.valueOf(maxThreads);
        } else {
            System.out.println("the threads is incorrect, it will use 10 as default.");
            return 10;
        }
    }

    public int getVersion() {
        if (version.matches("[12]")) {
            return Integer.valueOf(version);
        } else {
            System.out.println("the version is incorrect, it will use 2 as default.");
            return 2;
        }
    }

    public boolean getWithParallel() {
        if (withParallel.matches("(true|false)")) {
            return Boolean.valueOf(withParallel);
        } else {
            System.out.println("the parallel is incorrect, it will use true as default.");
            return true;
        }
    }

    public int getLevel() {
        if (level.matches("[12]")) {
            return Integer.valueOf(level);
        } else {
            System.out.println("the level is incorrect, it will use 1 as default.");
            return 1;
        }
    }

    public String getProcess() {
        return process == null ? "" : process;
    }

    public int getUnitLen() {
        if (unitLen.matches("\\d+")) {
            return Integer.valueOf(unitLen);
        } else {
            System.out.println("the unit-len is incorrect, it will use 1000 as default.");
            return 1000;
        }
    }
}