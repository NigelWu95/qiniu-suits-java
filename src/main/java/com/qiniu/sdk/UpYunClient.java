package com.qiniu.sdk;

import com.qiniu.util.CharactersUtils;
import com.upyun.UpException;
import com.upyun.UpYunUtils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

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
            throws IOException, UpException {
        String uri = "/" + bucketName + "/" + (prefix == null ? "" : prefix);
        // 获取链接
        URL url = new URL("http://" + UpYunConfig.apiDomain + uri);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(config.connect_timeout);
        conn.setReadTimeout(config.read_timeout);
        conn.setRequestMethod(UpYunConfig.METHOD_GET);
        conn.setUseCaches(false);
        String date = getGMTDate();
        conn.setRequestProperty(UpYunConfig.DATE, date);
        conn.setRequestProperty(UpYunConfig.AUTHORIZATION, UpYunUtils.sign(UpYunConfig.METHOD_GET, date, uri, userName,
                password, null));
        conn.setRequestProperty("x-list-iter", marker);
        conn.setRequestProperty("x-list-limit", String.valueOf(limit));
        conn.connect();
        return conn;
    }

    /**
     * 获取 GMT 格式时间戳
     *
     * @return GMT 格式时间戳
     */
    private String getGMTDate() {
        SimpleDateFormat format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US);
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
        return format.format(new Date());
    }
}

