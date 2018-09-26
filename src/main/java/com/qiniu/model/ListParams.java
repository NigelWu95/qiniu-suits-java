package com.qiniu.model;

public class ListParams extends BaseParams {

    private String bucket;
    private String resultFileDir;
    private String processResultFileDir;
    private String threads;
    private String version;
    private String withParallel;
    private String level;
    private String process;

    public ListParams(String[] args) throws Exception {
        super(args);
        this.bucket = getParam("bucket");
        this.resultFileDir = getParam("result-path");
        this.processResultFileDir = getParam("process-path");
        this.threads = getParam("threads");
        this.version = getParam("v");
        this.withParallel = getParam("parallel");
        this.level = getParam("level");
        this.process = getParam("process");
        super.setSelfName("list");
    }

    public String getBucket() {
        return bucket;
    }

    public String getResultFileDir() {
        return resultFileDir;
    }

    public String getProcessResultFileDir() {
        return processResultFileDir;
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