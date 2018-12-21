package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

public class PfopParams extends QossParams {

    private String pipeline;

    public PfopParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        this.pipeline = entryParam.getParamValue("pipeline");
    }

    public String getPipeline() throws Exception {
        return pipeline;
    }
}
