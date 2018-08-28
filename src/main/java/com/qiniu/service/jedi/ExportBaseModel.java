package com.qiniu.service.jedi;

public class ExportBaseModel {

   private String key = "";
   private String format = "";
   private String url = "";

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String toString() {
        return this.key + "," + this.format + "," + this.url;
    }
}