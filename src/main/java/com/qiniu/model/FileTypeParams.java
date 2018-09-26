package com.qiniu.model;

import com.qiniu.util.DateUtils;

public class FileTypeParams extends BaseParams {

    private String bucket;
    private String targetType;
    private PointTimeParams pointTimeParams;

    public FileTypeParams(String[] args) throws Exception {
        super(args);
        pointTimeParams = new PointTimeParams(args);
        this.bucket = getParam("bucket");
        this.targetType = getParam("type");
    }

    public String getBucket() {
        return bucket;
    }

    public int getTargetType() throws Exception {
        if (targetType.matches("(0|1)")) {
            return Integer.valueOf(targetType);
        } else {
            throw new Exception("the direction is incorrect, please set it 0 or 1");
        }
    }

    public PointTimeParams getPointTimeParams() {
        return pointTimeParams;
    }
}