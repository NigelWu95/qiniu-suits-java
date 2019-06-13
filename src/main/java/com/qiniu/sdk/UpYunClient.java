package com.qiniu.sdk;

import com.qiniu.util.CharactersUtils;
import com.qiniu.util.DatetimeUtils;
import com.qiniu.util.OssUtils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class UpYunClient {

    private UpYunConfig config;
    // 操作员名
    private String userName;
    // 操作员密码
    private String password;

    /**
     * 初始化 UpYun 存储接口
     *
     * @param userName   操作员名称
     * @param password   密码，不需要MD5加密
     * @return UpYun object
     */
    public UpYunClient(UpYunConfig config, String userName, String password) throws Exception {
        this.config = config;
        this.userName = userName;
        this.password = CharactersUtils.md5(password);
    }

    public HttpURLConnection listFilesConnection(String bucketName, String prefix, String marker, int limit)
            throws IOException {
        String uri = "/" + bucketName + "/" + (prefix == null ? "" : prefix);
        // 获取链接
        URL url = new URL("http://" + UpYunConfig.apiDomain + uri);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(config.connectTimeout);
        conn.setReadTimeout(config.readTimeout);
        conn.setRequestMethod(UpYunConfig.METHOD_GET);
        conn.setUseCaches(false);
        String date = DatetimeUtils.getGMTDate();
        conn.setRequestProperty(UpYunConfig.DATE, date);
        conn.setRequestProperty(UpYunConfig.AUTHORIZATION, OssUtils.upYunSign(UpYunConfig.METHOD_GET, date, uri, userName,
                password, null));
        conn.setRequestProperty("x-list-iter", marker);
        conn.setRequestProperty("x-list-limit", String.valueOf(limit));
        conn.connect();
        return conn;
    }
}

