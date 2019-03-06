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
        unitLen = entryParam.getValue("unit-len", null);
        threads = entryParam.getValue("threads", null);
        retryCount = entryParam.getValue("retry-times", null);
        saveTotal = entryParam.getValue("save-total", null);
        resultPath = entryParam.getValue("result-path", null);
        resultFormat = entryParam.getValue("result-format", null);
        resultSeparator = entryParam.getValue("result-separator", null);
        rmFields = entryParam.getValue("rm-fields", null);
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

    public String getSourceType() {
        return sourceType;
    }
    public String getFilePath() throws IOException {
        if ("".equals(filePath)) throw new IOException("not set file path.");
        else if (filePath.startsWith("/")) return filePath;
        else return System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;
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
        if (resultFormat == null) {
            return "table";
        } else {
            return resultFormat;
        }
    }

    public String getResultSeparator() {
        if (resultSeparator == null) {
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
