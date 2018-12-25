package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;

public class PfopParams extends QossParams {

    private String pipeline;
    private String forcePublic;
    private String fopsIndex;

    public PfopParams(IEntryParam entryParam) throws IOException {
        super(entryParam);
        try { this.pipeline = entryParam.getParamValue("pipeline"); } catch (Exception e) { pipeline = ""; }
        try { this.forcePublic = entryParam.getParamValue("force-public"); } catch (Exception e) { forcePublic = ""; }
        try { this.fopsIndex = entryParam.getParamValue("fops-index"); } catch (Exception e) { fopsIndex = ""; }
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

    public String getFopsIndex() throws IOException {
        if ("json".equals(getParseType())) {
            if ("".equals(fopsIndex)) {
                throw new IOException("no incorrect json key index for pfop's fops.");
            } else {
                return fopsIndex;
            }
        } else if ("table".equals(getParseType())) {
            if ("".equals(fopsIndex)) {
                return "1";
            } else if (fopsIndex.matches("\\d")) {
                return fopsIndex;
            } else {
                throw new IOException("no incorrect fops index, it should be a number.");
            }
        } else {
            throw new IOException("no incorrect object key index for pfop's fops.");
        }
    }
}
