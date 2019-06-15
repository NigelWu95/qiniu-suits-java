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
     */
    public UpYunClient(UpYunConfig config, String userName, String password) {
        this.config = config;
        this.userName = userName;
        this.password = CharactersUtils.md5(password);
    }

    public HttpURLConnection listFilesConnection(String bucket, String prefix, String marker, int limit)
            throws IOException {
        String uri = "/" + bucket + "/" + (prefix == null ? "" : prefix);
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

    /**
     * 获取文件信息
     *
     * @param key 文件路径
     * @return FileItem 文件对象
     */
    public FileItem getFileInfo(String bucket, String key) throws IOException {
        String uri = "/" + bucket + "/" + key;
        // 获取链接
        URL url = new URL("http://" + UpYunConfig.apiDomain + uri);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(config.connectTimeout);
        conn.setReadTimeout(config.readTimeout);
        conn.setRequestMethod(UpYunConfig.METHOD_HEAD);
        conn.setUseCaches(false);
        String date = DatetimeUtils.getGMTDate();
        conn.setRequestProperty(UpYunConfig.DATE, date);
        conn.setRequestProperty(UpYunConfig.AUTHORIZATION, OssUtils.upYunSign(UpYunConfig.METHOD_HEAD, date, uri, userName,
                password, null));
        conn.connect();
        int code = conn.getResponseCode();
        if (code != 200) throw new IOException(code + " " + conn.getResponseMessage());
        FileItem fileItem = new FileItem();
        fileItem.key = key;
        try {
            fileItem.attribute = conn.getHeaderField(UpYunConfig.X_UPYUN_FILE_TYPE);
            fileItem.size = Long.valueOf(conn.getHeaderField(UpYunConfig.X_UPYUN_FILE_SIZE));
            fileItem.timeSeconds = Long.valueOf(conn.getHeaderField(UpYunConfig.X_UPYUN_FILE_DATE));
        } catch (NullPointerException | NumberFormatException e) {
            throw new IOException(conn.getResponseMessage() + "  " + e.getMessage());
        } finally {
            conn.disconnect();
        }
        return fileItem;
    }
}

