package com.qiniu.sdk;

public class UpYunConfig implements Cloneable {

    /**
     * 默认的编码格式
     */
    public static final String UTF8 = "UTF-8";
    public static final String AUTHORIZATION = "Authorization";
    public static final String DATE = "Date";
    public static final String METHOD_GET = "GET";
    public static final String apiDomain = "v0.api.upyun.com";

    // 默认的超时时间：30秒
    public static int DEFAULT_CONNECT_TIMEOUT = 30 * 1000;
    public static int DEFAULT_READ_TIMEOUT = 30 * 1000;
    public static int DEFAULT_WRITE_TIMEOUT = 30 * 1000;

    public int connect_timeout = DEFAULT_CONNECT_TIMEOUT;
    public int read_timeout = DEFAULT_READ_TIMEOUT;
    public int write_timeout = DEFAULT_WRITE_TIMEOUT;

    public UpYunConfig() {}

    @Override
    public UpYunConfig clone() throws CloneNotSupportedException {
        return (UpYunConfig) super.clone();
    }
}
