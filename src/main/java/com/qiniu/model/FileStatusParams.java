package com.qiniu.model;

public class FileStatusParams extends BaseParams {

    private String targetStatus;
    private PointTimeParams pointTimeParams;

    public FileStatusParams(String[] args) throws Exception {
        super(args);
        pointTimeParams = new PointTimeParams(args);
        this.targetStatus = getParamFromArgs("status");
        super.setSelfName("status");
    }

    public FileStatusParams(String configFileName) throws Exception {
        super(configFileName);
        pointTimeParams = new PointTimeParams(configFileName);
        this.targetStatus = getParamFromConfig("status");
        super.setSelfName("status");
    }

    public short getTargetStatus() throws Exception {
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