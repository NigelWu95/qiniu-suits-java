package com.qiniu.model.parameter;

import com.qiniu.util.StringUtils;

public class CommonParams extends BaseParams {

    private String resultFormat;
    private String resultFileDir;
    private String process = "";
    private String processBatch = "";
    private String maxThreads = "";

    public CommonParams(String[] args) throws Exception {
        super(args);
        try { this.resultFormat = getParamFromArgs("result-format"); } catch (Exception e) {}
        try { this.resultFileDir = getParamFromArgs("result-path"); } catch (Exception e) {}
        try { this.process = getParamFromArgs("process"); } catch (Exception e) {}
        try { this.processBatch = getParamFromArgs("process-batch"); } catch (Exception e) {}
        try { this.maxThreads = getParamFromArgs("max-threads"); } catch (Exception e) {}
    }

    public CommonParams(String configFileName) throws Exception {
        super(configFileName);
        try { this.resultFormat = getParamFromConfig("result-format"); } catch (Exception e) {}
        try { this.resultFileDir = getParamFromConfig("result-path"); } catch (Exception e) {}
        try { this.process = getParamFromConfig("process"); } catch (Exception e) {}
        try { this.processBatch = getParamFromConfig("process-batch"); } catch (Exception e) {}
        try { this.maxThreads = getParamFromConfig("max-threads"); } catch (Exception e) {}
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
        if (processBatch.matches("(true|false)")) {
            return Boolean.valueOf(processBatch);
        } else {
            System.out.println("no incorrect process-batch status, it will use true as default.");
            return true;
        }
    }

    public int getMaxThreads() {
        if (maxThreads.matches("[1-9]\\d*")) {
            return Integer.valueOf(maxThreads);
        } else {
            System.out.println("no incorrect threads, it will use 10 as default.");
            return 10;
        }
    }
}
