package com.qiniu.model.parameter;

import com.qiniu.config.CommandArgs;
import com.qiniu.config.PropertyConfig;
import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CommonParams {

    protected IEntryParam entryParam;
    private String filePath;
    private String unitLen;
    private String threads;
    private String retryCount;
    private String saveTotal;
    private String resultPath;
    private String resultFormat;
    private String resultSeparator;
    private String rmFields;
    private String process;

    public CommonParams(IEntryParam entryParam) {
        this.entryParam = entryParam;
        try { this.filePath = entryParam.getParamValue("file-path");} catch (Exception e) { filePath = ""; }
        try { this.unitLen = entryParam.getParamValue("unit-len"); } catch (Exception e) { unitLen = ""; }
        try { this.threads = entryParam.getParamValue("threads"); } catch (Exception e) { threads = ""; }
        try { this.retryCount = entryParam.getParamValue("retry-times"); } catch (Exception e) { retryCount = ""; }
        try { this.saveTotal = entryParam.getParamValue("save-total"); } catch (Exception e) { saveTotal = ""; }
        try { this.resultPath = entryParam.getParamValue("result-path"); } catch (Exception e) { resultPath = ""; }
        try { this.resultFormat = entryParam.getParamValue("result-format"); } catch (Exception e) {}
        try { this.resultSeparator = entryParam.getParamValue("result-separator"); } catch (Exception e) {}
        try { this.rmFields = entryParam.getParamValue("rm-fields"); } catch (Exception e) { rmFields = ""; }
        try { this.process = entryParam.getParamValue("process"); } catch (Exception e) { process = ""; }
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
        String sourceType;
        try {
            sourceType = entryParam.getParamValue("source-type");
        } catch (IOException e1) {
            try {
                sourceType = entryParam.getParamValue("source");
            } catch (IOException e2) {
                if ("".equals(filePath)) sourceType = "file";
                else sourceType = "list";
            }
        }

        return sourceType;
    }

    public String getFilePath() throws IOException {
        if ("".equals(filePath)) throw new IOException("not set file path.");
        else if (filePath.startsWith("/")) throw new IOException("the file path only support relative path.");
        return filePath;
    }

    public int getUnitLen() {
        if (unitLen.matches("\\d+")) {
            return Integer.valueOf(unitLen);
        } else {
            return 10000;
        }
    }

    public int getThreads() {
        if (threads.matches("[1-9]\\d*")) {
            return Integer.valueOf(threads);
        } else {
            return 30;
        }
    }

    public int getRetryCount() {
        if (retryCount.matches("\\d+")) {
            return Integer.valueOf(retryCount);
        } else {
            return 3;
        }
    }

    public Boolean getSaveTotal() {
        if (saveTotal.matches("(true|false)")) {
            return Boolean.valueOf(saveTotal);
        } else {
            return "list".equals(getSourceType());
        }
    }

    public String getResultPath() throws IOException {
        if (resultPath.startsWith("/")) throw new IOException("the file path only support relative path.");
        else if ("".equals(resultPath)) {
            return "result";
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

    public List<String> getRmFields() {
        return Arrays.asList(rmFields.split(","));
    }

    public String getProcess() {
        return process;
    }
}
