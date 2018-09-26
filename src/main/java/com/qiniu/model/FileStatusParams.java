package com.qiniu.model;

public class FileStatusParams extends BaseParams {

    private String bucket;
    private String targetStatus;
    private PointTimeParams pointTimeParams;

    public FileStatusParams(String[] args) throws Exception {
        super(args);
        pointTimeParams = new PointTimeParams(args);
        this.bucket = getParam("bucket");
        this.targetStatus = getParam("status");
        super.setSelfName("status");
    }

    public String getBucket() {
        return bucket;
    }

    public short getTargetType() throws Exception {
        if (targetStatus.matches("(0|1)")) {
            return Short.valueOf(targetStatus);
        } else {
            throw new Exception("the direction is incorrect, please set it 0 or 1");
        }
    }

    public PointTimeParams getPointTimeParams() {
        return pointTimeParams;
    }
}