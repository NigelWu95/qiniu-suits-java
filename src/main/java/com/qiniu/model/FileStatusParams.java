package com.qiniu.model;

public class FileStatusParams extends BaseParams {

    private String targetStatus;

    public FileStatusParams(String[] args) throws Exception {
        super(args);
        this.targetStatus = getParamFromArgs("status");
    }

    public FileStatusParams(String configFileName) throws Exception {
        super(configFileName);
        this.targetStatus = getParamFromConfig("status");
    }

    public int getTargetStatus() throws Exception {
        if (targetStatus.matches("(0|1)")) {
            return Short.valueOf(targetStatus);
        } else {
            throw new Exception("the direction is incorrect, please set it 0 or 1");
        }
    }
}
