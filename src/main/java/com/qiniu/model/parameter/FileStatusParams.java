package com.qiniu.model.parameter;

public class FileStatusParams extends QossParams {

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
            throw new Exception("no incorrect status, please set it 0 or 1");
        }
    }
}
