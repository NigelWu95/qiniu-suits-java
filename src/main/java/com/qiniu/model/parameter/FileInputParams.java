package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;

public class FileInputParams extends CommonParams {

    private String filePath;
    private String separator;

    public FileInputParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        this.filePath = entryParam.getParamValue("file-path");
        try { this.separator = entryParam.getParamValue("separator"); } catch (Exception e) {}
    }

    public String getFilePath() throws IOException {
        if (filePath == null || "".equals(filePath)) throw new IOException("please set the file path.");
        else if (filePath.startsWith("/")) throw new IOException("the file path only support relative path.");
        return filePath;
    }

    public String getSeparator() {
        if (separator == null || "".equals(separator)) {
            return "\t";
        } else {
            return separator;
        }
    }

    public Boolean getSaveTotal() {
        if (saveTotal.matches("(true|false)")) {
            return Boolean.valueOf(saveTotal);
        } else {
            return false;
        }
    }
}
