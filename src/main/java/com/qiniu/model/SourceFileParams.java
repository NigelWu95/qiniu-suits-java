package com.qiniu.model;

import com.qiniu.util.StringUtils;

public class SourceFileParams extends CommonParams {

    private String separator;
    private String filePath;

    public SourceFileParams(String[] args) throws Exception {
        super(args);
        try { this.separator = getParamFromArgs("separator"); } catch (Exception e) { this.separator = ""; }
        this.filePath = getParamFromArgs("file-path");
    }

    public SourceFileParams(String configFileName) throws Exception {
        super(configFileName);
        try { this.separator = getParamFromConfig("separator"); } catch (Exception e) { this.separator = ""; }
        this.filePath = getParamFromConfig("file-path");
    }

    public String getSeparator() {
        if (StringUtils.isNullOrEmpty(separator)) {
            System.out.println("no incorrect separator, it will use \"\t\" as default.");
            return "\t";
        } else {
            return separator;
        }
    }

    public String getFilePath() {
        return filePath;
    }
}