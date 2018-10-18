package com.qiniu.model;

public class FileTypeParams extends BaseParams {

    private String targetType;

    public FileTypeParams(String[] args) throws Exception {
        super(args);
        this.targetType = getParamFromArgs("type");
    }

    public FileTypeParams(String configFileName) throws Exception {
        super(configFileName);
        this.targetType = getParamFromConfig("type");
    }

    public int getTargetType() throws Exception {
        if (targetType.matches("(0|1)")) {
            return Integer.valueOf(targetType);
        } else {
            throw new Exception("the direction is incorrect, please set it 0 or 1");
        }
    }
}