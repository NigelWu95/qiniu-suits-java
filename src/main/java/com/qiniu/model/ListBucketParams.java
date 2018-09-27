package com.qiniu.model;

public class ListBucketParams extends BaseParams {

    private String threads;
    private String version;
    private String withParallel;
    private String level;
    private String process;

    public ListBucketParams(String[] args) throws Exception {
        super(args);
        this.threads = getParamFromArgs("threads");
        this.version = getParamFromArgs("v");
        this.withParallel = getParamFromArgs("parallel");
        this.level = getParamFromArgs("level");
        this.process = getParamFromArgs("process");
        super.setSelfName("list");
    }

    public ListBucketParams(String configFileName) throws Exception {
        super(configFileName);
        this.threads = getParamFromConfig("threads");
        this.version = getParamFromConfig("v");
        this.withParallel = getParamFromConfig("parallel");
        this.level = getParamFromConfig("level");
        this.process = getParamFromConfig("process");
        super.setSelfName("list");
    }

    public int getThreads() {
        if (threads.matches("[1-9]\\d*")) {
            return Integer.valueOf(threads);
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
        return process;
    }
}