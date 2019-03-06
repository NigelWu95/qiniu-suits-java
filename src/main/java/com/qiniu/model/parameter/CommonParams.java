package com.qiniu.model.parameter;

import com.qiniu.config.CommandArgs;
import com.qiniu.config.FileProperties;
import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CommonParams {

    protected IEntryParam entryParam;
    private String sourceType;
    private String filePath;
    private int unitLen;
    private int threads;
    private int retryCount;
    private boolean saveTotal;
    private String resultPath;
    private String resultFormat;
    private String resultSeparator;
    private List<String> rmFields;
    private String process;

    public CommonParams(IEntryParam entryParam) {
        this.entryParam = entryParam;
        try {
            sourceType = entryParam.getValue("source-type");
        } catch (IOException e1) {
            try {
                sourceType = entryParam.getValue("source");
            } catch (IOException e2) {
                if ("".equals(filePath)) sourceType = "list";
                else sourceType = "file";
            }
        }
        filePath = entryParam.getValue("file-path", null);

        setUnitLen(entryParam.getValue("unit-len", null));
        setThreads(entryParam.getValue("threads", null));
        setRetryCount(entryParam.getValue("retry-times", null));
        setSaveTotal(entryParam.getValue("save-total", null));
        resultPath = entryParam.getValue("result-path", "result");
        resultFormat = entryParam.getValue("result-format", "table");
        resultSeparator = entryParam.getValue("result-separator", "\t");
        rmFields = Arrays.asList(entryParam.getValue("rm-fields", "").split(","));
        process = entryParam.getValue("process", null);
    }

    public CommonParams(String[] args) throws IOException {
        this(new CommandArgs(args));
    }

    public CommonParams(String configFileName) throws IOException {
        this(new FileProperties(configFileName));
    }

    public String getParamByKey(String key) throws IOException {
        return entryParam.getValue(key);
    }

    private void setFilePath(String filePath) throws IOException {
        if ("".equals(filePath)) {
            throw new IOException("not set file path.");
        } else {
            this.filePath = filePath;
        }
    }

    private void setUnitLen(String unitLen) {
        if (unitLen.matches("\\d+")) {
            this.unitLen = Integer.valueOf(unitLen);
        } else {
            this.unitLen = 10000;
        }
    }

    private void setThreads(String threads) {
        if (threads.matches("[1-9]\\d*")) {
            this.threads = Integer.valueOf(threads);
        } else {
            this.threads = 30;
        }
    }

    private void setRetryCount(String retryCount) {
        if (retryCount.matches("\\d+")) {
            this.retryCount = Integer.valueOf(retryCount);
        } else {
            this.retryCount = 3;
        }
    }

    private void setSaveTotal(String saveTotal) {
        if (saveTotal.matches("(true|false)")) {
            this.saveTotal = Boolean.valueOf(saveTotal);
        } else {
            this.saveTotal = "list".equals(getSourceType());
        }
    }

    public String getSourceType() {
        return sourceType;
    }

    public String getFilePath() {
        return filePath;
    }

    public int getUnitLen() {
        return unitLen;
    }

    public int getThreads() {
        return threads;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public Boolean getSaveTotal() {
        return saveTotal;
    }

    public String getResultPath() {
        return resultPath;
    }

    public String getResultFormat() {
        return resultFormat;
    }

    public String getResultSeparator() {
        return resultSeparator;
    }

    public List<String> getRmFields() {
        return rmFields;
    }

    public String getProcess() {
        return process;
    }
}
