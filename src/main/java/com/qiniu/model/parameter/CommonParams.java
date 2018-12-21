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
    private String resultFileDir;
    private String resultFormat;
    private String resultSeparator;
    protected String saveTotal;
    private String process;
    private String processBatch;
    private String maxThreads;

    public CommonParams(IEntryParam entryParam) {
        this.entryParam = entryParam;
        try { this.sourceType = entryParam.getParamValue("source-type"); } catch (Exception e) {}
        try { this.unitLen = entryParam.getParamValue("unit-len"); } catch (Exception e) { unitLen = ""; }
        try { this.retryCount = entryParam.getParamValue("retry-times"); } catch (Exception e) { retryCount = ""; }
        try { this.resultFileDir = entryParam.getParamValue("result-path"); } catch (Exception e) {}
        try { this.resultFormat = entryParam.getParamValue("result-format"); } catch (Exception e) {}
        try { this.resultSeparator = entryParam.getParamValue("result-separator"); } catch (Exception e) {}
        try { this.saveTotal = entryParam.getParamValue("save-total"); } catch (Exception e) { saveTotal = ""; }
        try { this.process = entryParam.getParamValue("process"); } catch (Exception e) {}
        try { this.processBatch = entryParam.getParamValue("process-batch"); } catch (Exception e) { processBatch = ""; }
        try { this.maxThreads = entryParam.getParamValue("max-threads"); } catch (Exception e) { maxThreads = ""; }
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
            System.out.println("no incorrect unit-len, it will use 1000 as default.");
            return 1000;
        }
    }

    public int getRetryCount() {
        if (retryCount.matches("\\d+")) {
            return Integer.valueOf(retryCount);
        } else {
            System.out.println("no incorrect retry times, it will use 3 as default.");
            return 3;
        }
    }

    public String getResultFileDir() {
        if (resultFileDir == null || "".equals(resultFileDir)) {
            System.out.println("no incorrect result file directory, it will use \"../result\" as default.");
            return "../result";
        }
        return System.getProperty("user.dir") + System.getProperty("file.separator") + resultFileDir;
    }

    public String getResultFormat() {
        if (resultFormat == null || "".equals(resultFormat)) {
            System.out.println("no incorrect result format, it will use \"json\" as default.");
            return "json";
        } else {
            return resultFormat;
        }
    }

    public String getResultSeparator() {
        if (resultSeparator == null || "".equals(resultSeparator)) {
            System.out.println("no incorrect result separator, it will use \"\t\" as default.");
            return "\t";
        } else {
            return resultSeparator;
        }
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
