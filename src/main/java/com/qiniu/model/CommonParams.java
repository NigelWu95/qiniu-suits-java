package com.qiniu.model;

import com.qiniu.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CommonParams extends BaseParams {

    private String resultFormat;
    private String resultFileDir;
    private String process;
    private String processBatch;

    public CommonParams(String[] args) throws Exception {
        super(args);
        try { this.resultFormat = getParamFromArgs("result-format"); } catch (Exception e) { this.resultFormat = ""; }
        try { this.resultFileDir = getParamFromArgs("result-path"); } catch (Exception e) { this.resultFileDir = ""; }
        try { this.process = getParamFromArgs("process"); } catch (Exception e) { this.process = ""; }
        try { this.processBatch = getParamFromArgs("process-batch"); } catch (Exception e) {}
    }

    public CommonParams(String configFileName) throws Exception {
        super(configFileName);
        try { this.resultFormat = getParamFromConfig("result-format"); } catch (Exception e) { this.resultFormat = ""; }
        try { this.resultFileDir = getParamFromConfig("result-path"); } catch (Exception e) { this.resultFileDir = ""; }
        try { this.process = getParamFromConfig("process"); } catch (Exception e) { this.process = ""; }
        try { this.processBatch = getParamFromConfig("process-batch"); } catch (Exception e) {}
    }

    public String getResultFormat() {
        if (StringUtils.isNullOrEmpty(resultFormat)) {
            System.out.println("no incorrect result format, it will use \"json\" as default.");
            return "json";
        } else {
            return resultFormat;
        }
    }

    public String getResultFileDir() {
        if (StringUtils.isNullOrEmpty(resultFileDir)) {
            System.out.println("no incorrect result file directory, it will use \"../result\" as default.");
            this.resultFileDir = "../result";
        }
        return System.getProperty("user.dir") + System.getProperty("file.separator") + resultFileDir;
    }

    public String getProcess() {
        return process;
    }

    public boolean getProcessBatch() {
        if (StringUtils.isNullOrEmpty(processBatch) || !processBatch.matches("(true|false)")) {
            System.out.println("no incorrect process-batch status, it will use false as default.");
            return false;
        } else {
            return Boolean.valueOf(processBatch);
        }
    }
}