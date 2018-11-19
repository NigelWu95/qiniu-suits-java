package com.qiniu.model;

import com.qiniu.common.QiniuException;
import com.qiniu.util.StringUtils;

public class FileInputParams extends CommonParams {

    private String separator;
    private String filePath;
    private String keyIndex;

    public FileInputParams(String[] args) throws Exception {
        super(args);
        try { this.separator = getParamFromArgs("separator"); } catch (Exception e) {}
        this.filePath = getParamFromArgs("file-path");
        try { this.keyIndex = getParamFromArgs("key-index"); } catch (Exception e) {}
    }

    public FileInputParams(String configFileName) throws Exception {
        super(configFileName);
        try { this.separator = getParamFromConfig("separator"); } catch (Exception e) { this.separator = ""; }
        this.filePath = getParamFromConfig("file-path");
        try { this.keyIndex = getParamFromConfig("key-index"); } catch (Exception e) {}
    }

    public String getSeparator() {
        if (StringUtils.isNullOrEmpty(separator)) {
            System.out.println("no incorrect separator, it will use \"\t\" as default.");
            return "\t";
        } else {
            return separator;
        }
    }

    public String getFilePath() throws QiniuException {
        if (StringUtils.isNullOrEmpty(filePath)) {
            throw new QiniuException(null, "no incorrect file-path, please set it.");
        } else {
            return filePath;
        }
    }

    public int getKeyIndex() {
        if (keyIndex.matches("[\\d]")) {
            return Integer.valueOf(keyIndex);
        } else {
            System.out.println("no incorrect key index, it will use 0 as default.");
            return 0;
        }
    }
}
