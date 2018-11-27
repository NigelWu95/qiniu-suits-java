package com.qiniu.service.media;

public class VideoTS {

    private String url;
    private float seconds;

    public VideoTS(String url, float seconds) {
        this.url = url;
        this.seconds = seconds;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public float getSeconds() {
        return seconds;
    }

    public void setSeconds(float seconds) {
        this.seconds = seconds;
    }

    public String toString() {
        return url + ": " + seconds + "sec";
    }

}
