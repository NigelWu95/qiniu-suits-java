package com.qiniu.model;

public class FileTypeParams extends BaseParams {

    private String targetType;
    private PointTimeParams pointTimeParams;

    public FileTypeParams(String[] args) throws Exception {
        super(args);
        pointTimeParams = new PointTimeParams(args);
        this.targetType = getParamFromArgs("type");
        super.setSelfName("type");
    }

    public FileTypeParams(String configFileName) throws Exception {
        super(configFileName);
        pointTimeParams = new PointTimeParams(configFileName);
        this.targetType = getParamFromArgs("type");
        super.setSelfName("type");
    }

    public short getTargetType() throws Exception {
        if (targetType.matches("(0|1)")) {
            return Short.valueOf(targetType);
        } else {
            throw new Exception("the direction is incorrect, please set it 0 or 1");
        }
    }

    public PointTimeParams getPointTimeParams() {
        return pointTimeParams;
    }
}