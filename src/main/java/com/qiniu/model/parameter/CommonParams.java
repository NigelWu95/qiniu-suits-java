package com.qiniu.model.parameter;

import com.qiniu.config.CommandArgs;
import com.qiniu.config.PropertiesFile;
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

    public CommonParams(IEntryParam entryParam) throws IOException {
        this.entryParam = entryParam;
        try {
            sourceType = entryParam.getParamValue("source-type");
        } catch (IOException e1) {
            try {
                sourceType = entryParam.getParamValue("source");
            } catch (IOException e2) {
                if ("".equals(filePath)) sourceType = "list";
                else sourceType = "file";
            }
        }
        try { filePath = entryParam.getParamValue("file-path");} catch (Exception e) { filePath = ""; }
        try { unitLen = entryParam.getParamValue("unit-len"); } catch (Exception e) { unitLen = ""; }
        try { threads = entryParam.getParamValue("threads"); } catch (Exception e) { threads = ""; }
        try { retryCount = entryParam.getParamValue("retry-times"); } catch (Exception e) { retryCount = ""; }
        try { saveTotal = entryParam.getParamValue("save-total"); } catch (Exception e) { saveTotal = ""; }
        try { resultPath = entryParam.getParamValue("result-path"); } catch (Exception e) { resultPath = ""; }
        try { resultFormat = entryParam.getParamValue("result-format"); } catch (Exception e) {}
        try { resultSeparator = entryParam.getParamValue("result-separator"); } catch (Exception e) {}
        try { rmFields = entryParam.getParamValue("rm-fields"); } catch (Exception e) { rmFields = ""; }
        try { process = entryParam.getParamValue("process"); } catch (Exception e) { process = ""; }

        try {
            filePath = entryParam.getParamValue("file-path");
            unitLen = entryParam.getParamValue("unit-len");
            threads = entryParam.getParamValue("threads");
            retryCount = entryParam.getParamValue("retry-times");
            saveTotal = entryParam.getParamValue("save-total");
            resultPath = entryParam.getParamValue("result-path");
            resultFormat = entryParam.getParamValue("result-format");
            resultSeparator = entryParam.getParamValue("result-separator");
            rmFields = entryParam.getParamValue("rm-fields");
            process = entryParam.getParamValue("process");
        } catch (Exception ignored) {}

        filePath = checkedFilePath();
        unitLen = checkedUnitLen();
        threads = checkedThreads();
        retryCount = checkedRetryCount();
        saveTotal = entryParam.getParamValue("save-total");
        resultPath = entryParam.getParamValue("result-path");
        resultFormat = entryParam.getParamValue("result-format");
        resultSeparator = entryParam.getParamValue("result-separator");
        rmFields = entryParam.getParamValue("rm-fields");
        process = entryParam.getParamValue("process");
    }

    public CommonParams(String[] args) throws IOException {
        this(new CommandArgs(args));
    }

    public CommonParams(String configFileName) throws IOException {
        this(new PropertiesFile(configFileName));
    }

    public String getParamByKey(String key) throws IOException {
        return entryParam.getParamValue(key);
    }

    public String getSourceType() {
        return sourceType;
    }

    private String checkedFilePath() throws IOException {
        if ("".equals(filePath)) throw new IOException("not set file path.");
        else if (filePath.startsWith("/")) throw new IOException("the file path only support relative path.");
        return filePath;
    }

    private String checkedInt(String param, String defaultValue) {
        if (param == null || "".equals(param)) {

        }
        if (param.matches("\\d+")) {
            return param;
        } else {
            return defaultValue;
        }
    }

    private String checkedUnitLen() {
        if (unitLen.matches("\\d+")) {
            return unitLen;
        } else {
            return "10000";
        }
    }

    private String checkedThreads() {
        if (threads.matches("[1-9]\\d*")) {
            return threads;
        } else {
            return "30";
        }
    }

    private String checkedRetryCount() {
        if (retryCount.matches("\\d+")) {
            return retryCount;
        } else {
            return "3";
        }
    }

    private Boolean checkedSaveTotal() {
        if (saveTotal.matches("(true|false)")) {
            return Boolean.valueOf(saveTotal);
        } else {
            return "list".equals(getSourceType());
        }
    }

    private String checkedResultPath() throws IOException {
        if (resultPath.startsWith("/")) throw new IOException("the file path only support relative path.");
        else if ("".equals(resultPath)) {
            return "result";
        }
        return System.getProperty("user.dir") + System.getProperty("file.separator") + resultPath;
    }

    private String checkedResultFormat() {
        if (resultFormat == null || "".equals(resultFormat)) {
            return "table";
        } else {
            return resultFormat;
        }
    }

    private String checkedResultSeparator() {
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
