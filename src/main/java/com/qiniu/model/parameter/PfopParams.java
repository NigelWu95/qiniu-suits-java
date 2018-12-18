package com.qiniu.model.parameter;

public class PfopParams extends QossParams {

    private String pipeline;

    public PfopParams(String[] args) throws Exception {
        super(args);
        this.pipeline = getParamFromArgs("pipeline");
    }

    public PfopParams(String configFileName) throws Exception {
        super(configFileName);
        this.pipeline = getParamFromConfig("pipeline");
    }

    public String getPipeline() throws Exception {
        return pipeline;
    }
}
