package com.qiniu.model.parameter;

import com.qiniu.config.CommandArgs;
import com.qiniu.config.PropertyConfig;
import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;

public class CommonParams {

    protected IEntryParam entryParam;
    private String sourceType;
    private String unitLen;
    private String retryCount;
    private String resultPath;
    private String resultFormat;
    private String resultSeparator;
    protected String saveTotal;
    private String process;
    private String maxThreads;

    public CommonParams(IEntryParam entryParam) throws IOException {
        this.entryParam = entryParam;
        this.sourceType = entryParam.getParamValue("source-type");
        try { this.unitLen = entryParam.getParamValue("unit-len"); } catch (Exception e) { unitLen = ""; }
        try { this.retryCount = entryParam.getParamValue("retry-times"); } catch (Exception e) { retryCount = ""; }
        try { this.resultPath = entryParam.getParamValue("result-path"); } catch (Exception e) { resultPath = ""; }
        try { this.resultFormat = entryParam.getParamValue("result-format"); } catch (Exception e) {}
        try { this.resultSeparator = entryParam.getParamValue("result-separator"); } catch (Exception e) {}
        try { this.saveTotal = entryParam.getParamValue("save-total"); } catch (Exception e) { saveTotal = ""; }
        try { this.process = entryParam.getParamValue("process"); } catch (Exception e) { process = ""; }
        try { this.maxThreads = entryParam.getParamValue("threads"); } catch (Exception e) { maxThreads = ""; }
    }

    public CommonParams(String[] args) throws IOException {
        this(new CommandArgs(args));
    }

    public CommonParams(String configFileName) throws IOException {
        this(new PropertyConfig(configFileName));
    }

    public String getParamByKey(String key) throws IOException {
        return entryParam.getParamValue(key);
    }

    public String getSourceType() {
        return sourceType;
    }

    public int getUnitLen() {
        if (unitLen.matches("\\d+")) {
            return Integer.valueOf(unitLen);
        } else {
            return 10000;
        }
    }

    public int getRetryCount() {
        if (retryCount.matches("\\d+")) {
            return Integer.valueOf(retryCount);
        } else {
            return 3;
        }
    }

    public String getResultPath() throws IOException {
        if (resultPath.startsWith("/")) throw new IOException("the file path only support relative path.");
        else if ("".equals(resultPath)) {
            return "../result";
        }
        return System.getProperty("user.dir") + System.getProperty("file.separator") + resultPath;
    }

    public String getResultFormat() {
        if (resultFormat == null || "".equals(resultFormat)) {
            return "json";
        } else {
            return resultFormat;
        }
    }

    public String getResultSeparator() {
        if (resultSeparator == null || "".equals(resultSeparator)) {
            return "\t";
        } else {
            return resultSeparator;
        }
    }

    public String getProcess() {
        return process;
    }

    public int getMaxThreads() {
        if (maxThreads.matches("[1-9]\\d*")) {
            return Integer.valueOf(maxThreads);
        } else {
            return 30;
        }
    }
}
