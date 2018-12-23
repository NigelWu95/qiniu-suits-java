package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;

public class PfopParams extends QossParams {

    private String pipeline;
    private String forcePublic;

    public PfopParams(IEntryParam entryParam) throws IOException {
        super(entryParam);
        try { this.pipeline = entryParam.getParamValue("pipeline"); } catch (Exception e) { pipeline = ""; }
        try { this.forcePublic = entryParam.getParamValue("force-public"); } catch (Exception e) { forcePublic = ""; }
    }

    public String getPipeline() throws IOException {
        boolean force = false;
        if (forcePublic.matches("(true|false)")) {
            force = Boolean.valueOf(forcePublic);
        } else if (!"".equals(forcePublic)) {
            throw new IOException("please set force-public as true/false.");
        }
        if ("".equals(pipeline) && !force) throw new IOException("please set pipeline, if you don't want to use" +
                " private pipeline, please set the force-public as true.");
        return pipeline;
    }
}
