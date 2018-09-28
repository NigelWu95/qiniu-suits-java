package com.qiniu.service.fileline;

import com.qiniu.common.QiniuSuitsException;
import com.qiniu.interfaces.ILineParser;
import com.qiniu.util.LineUtils;

public class ChangeTypeErrorLineParser implements ILineParser {

    private String delimiter;

    private String error;

    private String bucket;

    private String key;

    private String type;

    public ChangeTypeErrorLineParser(String delimiter) {
        this.delimiter = delimiter;
    }

    public void splitLine(String line) throws QiniuSuitsException {
        String[] items = line.split(delimiter);
        this.error = LineUtils.getIndexItem(items, 0);
        this.bucket = LineUtils.getIndexItem(items, 1);
        this.key = LineUtils.getIndexItem(items, 2);
        this.type = LineUtils.getIndexItem(items, 3);
    }

    public String getError() {
        return error;
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    public String getType() {
        return type;
    }

    public String toString() {
        return "{\"error\":\"" + error + "\",\"bucket\":\"" + bucket + "\",\"key\":\"" + key + "\",\"type\":\"" + type + "\"}";
    }
}