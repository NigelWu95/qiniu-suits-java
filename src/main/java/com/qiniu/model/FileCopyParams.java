package com.qiniu.model;

import com.qiniu.util.DateUtils;

public class FileCopyParams extends BaseParams {

    private String sourceBucket;
    private String targetBucket;
    private String keepKey;
    private String targetKeyPrefix;
    private PointTimeParams pointTimeParams;

    public FileCopyParams(String[] args) throws Exception {
        super(args);
        pointTimeParams = new PointTimeParams(args);
        this.sourceBucket = getParam("from");
        this.targetBucket = getParam("to");
        this.keepKey = getParam("keep-key");
        this.targetKeyPrefix = getParam("add-prefix");
    }

    public String getSourceBucket() {
        return sourceBucket;
    }

    public String getTargetBucket() {
        return targetBucket;
    }

    public boolean getKeepKey() {
        if (keepKey.matches("(true|false)")) {
            return Boolean.valueOf(keepKey);
        } else {
            System.out.println("the keep-key is incorrect, it will use true as default.");
            return true;
        }
    }

    public String getTargetKeyPrefix() {
        return targetKeyPrefix;
    }

    public PointTimeParams getPointTimeParams() {
        return pointTimeParams;
    }
}