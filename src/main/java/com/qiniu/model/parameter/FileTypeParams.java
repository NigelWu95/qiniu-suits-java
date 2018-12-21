package com.qiniu.model.parameter;

public class FileTypeParams extends QossParams {

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
            throw new Exception("no incorrect type, please set it 0 or 1.");
        }
    }
}
