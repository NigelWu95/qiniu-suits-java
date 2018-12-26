package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileInputParams extends CommonParams {

    private String filePath;
    private String separator;
    private String indexes;

    public FileInputParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        this.filePath = entryParam.getParamValue("file-path");
        try { this.separator = entryParam.getParamValue("in-separator"); } catch (Exception e) {}
        try { this.indexes = entryParam.getParamValue("indexes"); } catch (Exception e) { indexes = ""; }
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

    public Map<String, String> getIndexMap() throws IOException {
        List<String> keys = Arrays.asList("key", "hash", "fsize", "putTime", "mimeType", "endUser", "type", "status");
        if ("table".equals(getParseType())) {
            if ("".equals(indexes) || indexes.matches("(\\d+,)*\\d")) {
                List<String> indexList = Arrays.asList(indexes.split(","));
                if (indexList.size() == 0) {
                    return new HashMap<String, String>(){{ put("0", keys.get(0)); }};
                } else if (indexList.size() > 8) {
                    throw new IOException("the file info's index length is too long.");
                } else {
                    return new HashMap<String, String>(){{
                        for (int i = 0; i < indexList.size(); i++) { put(indexList.get(i), keys.get(i)); }
                    }};
                }
            } else {
                throw new IOException("the index pattern is not supported.");
            }
        } else {
            List<String> indexList = Arrays.asList(indexes.split(","));
            if (indexList.size() == 0) {
                return new HashMap<String, String>(){{ put("key", keys.get(0)); }};
            } else if (indexList.size() > 8) {
                throw new IOException("the file info's index length is too long.");
            } else {
                return new HashMap<String, String>(){{
                    for (int i = 0; i < indexList.size(); i++) put(indexList.get(i), keys.get(i));
                }};
            }
        }
    }
}
