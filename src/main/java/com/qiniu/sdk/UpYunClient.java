package com.qiniu.sdk;

import com.qiniu.common.SuitsException;
import com.qiniu.util.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class UpYunClient {

    private UpYunConfig config;
    // 操作员名
    private String userName;
    // 操作员密码
    private String password;

    public UpYunClient(UpYunConfig config, String userName, String password) {
        this.config = config;
        this.userName = userName;
        this.password = CharactersUtils.md5(password);
    }

    public String listFiles(String bucket, String directory, String marker, int limit) throws IOException {
        String uri = "/" + bucket + "/" + URLUtils.getSpaceEscapedURI(directory);
        URL url = new URL("http://" + UpYunConfig.apiDomain + uri);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(config.connectTimeout);
        conn.setReadTimeout(config.readTimeout);
        conn.setRequestMethod(UpYunConfig.METHOD_GET);
        conn.setUseCaches(false);
        String date = DatetimeUtils.getGMTDate();
        conn.setRequestProperty(UpYunConfig.DATE, date);
        conn.setRequestProperty(UpYunConfig.AUTHORIZATION, CloudAPIUtils.upYunSign(UpYunConfig.METHOD_GET, date, uri,
                userName, password, null));
        conn.setRequestProperty("x-list-iter", marker);
        conn.setRequestProperty("x-list-limit", String.valueOf(limit));
        conn.setRequestProperty("Accept", "application/json");
        conn.connect();
        int code = conn.getResponseCode();
//        is = conn.getInputStream(); // 状态码错误时不能使用 getInputStream()
        InputStream is = code >= 400 ? conn.getErrorStream() : conn.getInputStream();
        InputStreamReader sr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(sr);
        StringBuilder text = new StringBuilder();
        try {
            char[] chars = new char[4096];
            int length;
            while ((length = br.read(chars)) != -1) {
                text.append(chars, 0, length);
            }
        } finally {
            try {
                conn.disconnect();
                br.close();
                sr.close();
                is.close();
            } catch (IOException e) {
                br = null;
                sr = null;
                is = null;
            }
        }
        if (code == 200) {
            return text.toString();
        } else {
            throw new SuitsException(code, text.toString() + "\t" + directory + "\t" + marker);
        }
    }

    public FileItem getFileInfo(String bucket, String key) throws IOException {
        String uri = "/" + bucket + "/" + URLUtils.getSpaceEscapedURI(key);
        HttpURLConnection conn;
        URL url = new URL("http://" + UpYunConfig.apiDomain + uri);
        conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(config.connectTimeout);
        conn.setReadTimeout(config.readTimeout);
        conn.setRequestMethod(UpYunConfig.METHOD_HEAD);
        conn.setUseCaches(false);
        String date = DatetimeUtils.getGMTDate();
        conn.setRequestProperty(UpYunConfig.DATE, date);
        conn.setRequestProperty(UpYunConfig.AUTHORIZATION, CloudAPIUtils.upYunSign(UpYunConfig.METHOD_HEAD, date, uri,
                userName, password, null));
        conn.connect();
        int code = conn.getResponseCode();
        if (code != 200) throw new SuitsException(code, conn.getResponseMessage());
        FileItem fileItem = new FileItem();
        fileItem.key = key;
        try {
            fileItem.attribute = conn.getHeaderField(UpYunConfig.X_UPYUN_FILE_TYPE);
            fileItem.size = Long.valueOf(conn.getHeaderField(UpYunConfig.X_UPYUN_FILE_SIZE));
            fileItem.timeSeconds = Long.valueOf(conn.getHeaderField(UpYunConfig.X_UPYUN_FILE_DATE));
        } catch (NullPointerException | NumberFormatException e) {
            throw new SuitsException(e, 404, e.getMessage() + ", the file may be not exists: " + fileItem);
        } finally {
            conn.disconnect();
        }
        return fileItem;
    }
}

