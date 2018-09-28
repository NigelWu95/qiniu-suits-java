package com.qiniu.service.fileline;

import com.qiniu.common.QiniuSuitsException;
import com.qiniu.interfaces.ILineParser;
import com.qiniu.util.LineUtils;

public class SplitLineParser implements ILineParser {

    private String delimiter;

    private String url;

    private String key;

    public SplitLineParser(String delimiter) {
        this.delimiter = delimiter;
    }

    public void splitLine(String line) throws QiniuSuitsException {
        String[] items = line.split(delimiter);
        this.url = LineUtils.getIndexItem(items, 0);
        this.key = LineUtils.getIndexItem(items, 1);
    }

    public String getUrl() {
        return url;
    }

    public String getKey() {
        return key;
    }

    public String toString() {
        return "{\"url\":\"" + url + "\",\"key\":\"" + key + "\"}";
    }
}