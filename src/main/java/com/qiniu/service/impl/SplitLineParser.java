package com.qiniu.service.impl;

import com.qiniu.interfaces.ILineParser;

public class SplitLineParser implements ILineParser {

    private String key;
    private String url;

    public SplitLineParser(String line) {
        // 原列表文件格式为 url[\t]key
        this.url = line.split("\t")[0];
        this.key = line.split("\t")[1];
    }

    public String getUrl() {
        return this.url;
    }

    public String getKey() {
        return this.key;
    }

    public String toString() {
        return getUrl() + "," + getKey();
    }
}