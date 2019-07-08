package com.qiniu.sdk;

import com.qiniu.common.SuitsException;
import com.qiniu.util.CharactersUtils;
import com.qiniu.util.DatetimeUtils;
import com.qiniu.util.ListingUtils;
import com.qiniu.util.URLUtils;

import java.io.IOException;
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

    public HttpURLConnection listFilesConnection(String bucket, String directory) throws IOException {
        String uri = "/" + bucket + "/" + URLUtils.getSpaceEscapedURI(directory);
        URL url = new URL("http://" + UpYunConfig.apiDomain + uri);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(config.connectTimeout);
        conn.setReadTimeout(config.readTimeout);
        conn.setRequestMethod(UpYunConfig.METHOD_GET);
        conn.setUseCaches(false);
        String date = DatetimeUtils.getGMTDate();
        conn.setRequestProperty(UpYunConfig.DATE, date);
        conn.setRequestProperty(UpYunConfig.AUTHORIZATION, ListingUtils.upYunSign(UpYunConfig.METHOD_GET, date, uri,
                userName, password, null));
        return conn;
    }

    public FileItem getFileInfo(String bucket, String key) throws SuitsException {
        String uri = "/" + bucket + "/" + URLUtils.getSpaceEscapedURI(key);
        HttpURLConnection conn;
        try {
            URL url = new URL("http://" + UpYunConfig.apiDomain + uri);
            conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(config.connectTimeout);
            conn.setReadTimeout(config.readTimeout);
            conn.setRequestMethod(UpYunConfig.METHOD_HEAD);
            conn.setUseCaches(false);
            String date = DatetimeUtils.getGMTDate();
            conn.setRequestProperty(UpYunConfig.DATE, date);
            conn.setRequestProperty(UpYunConfig.AUTHORIZATION, ListingUtils.upYunSign(UpYunConfig.METHOD_HEAD, date, uri, userName,
                    password, null));
            conn.connect();
            int code = conn.getResponseCode();
            if (code != 200) throw new SuitsException(code, conn.getResponseMessage());
        } catch (IOException e) {
            throw new SuitsException(-1, e.getMessage());
        }
        FileItem fileItem = new FileItem();
        fileItem.key = key;
        try {
            fileItem.attribute = conn.getHeaderField(UpYunConfig.X_UPYUN_FILE_TYPE);
            fileItem.size = Long.valueOf(conn.getHeaderField(UpYunConfig.X_UPYUN_FILE_SIZE));
            fileItem.timeSeconds = Long.valueOf(conn.getHeaderField(UpYunConfig.X_UPYUN_FILE_DATE));
        } catch (NullPointerException | NumberFormatException e) {
            throw new SuitsException(404, e.getMessage() + ", the file may be not exists: " + fileItem);
        } finally {
            conn.disconnect();
        }
        return fileItem;
    }
}

