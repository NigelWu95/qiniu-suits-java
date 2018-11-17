package com.qiniu.model;

public class FetchFile {

    public String url;

    public String key;

    public String md5;

    public String etag;

    public FetchFile(String url, String key, String md5, String etag) {

        this.url = url;
        this.key = key;
        this.md5 = md5;
        this.etag = etag;
    }
}