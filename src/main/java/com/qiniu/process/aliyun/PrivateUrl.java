package com.qiniu.process.aliyun;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.process.Base;

import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.Map;

public class PrivateUrl extends Base<Map<String, String>> {

    private String endpoint;
    private Date expiration;
    private OSSClient ossClient;
    private ILineProcess<Map<String, String>> nextProcessor;

    public PrivateUrl(String accessKeyId, String accessKeySecret, String bucket, String endpoint, long expires,
                      String savePath, int saveIndex) throws IOException {
        super("aliprivate", accessKeyId, accessKeySecret, bucket, savePath, saveIndex);
        this.endpoint = endpoint;
        expiration = new Date(System.currentTimeMillis() + expires);
        ossClient = new OSSClient(endpoint, new DefaultCredentialProvider(accessKeyId, accessKeySecret), null);
    }

    public PrivateUrl(String accessKeyId, String accessKeySecret, String bucket, String endpoint, long expires) {
        super("aliprivate", accessKeyId, accessKeySecret, bucket);
        this.endpoint = endpoint;
        ossClient = new OSSClient(endpoint, new DefaultCredentialProvider(accessKeyId, accessKeySecret), null);
    }

    public PrivateUrl(String accessKeyId, String accessKeySecret, String bucket, String endpoint, long expires,
                      String savePath) throws IOException {
        this(accessKeyId, accessKeySecret, bucket, endpoint, expires, savePath, 0);
    }

    public void updateEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public void updateExpires(long expires) {
        this.expiration = new Date(System.currentTimeMillis() + expires);
    }

    public PrivateUrl clone() throws CloneNotSupportedException {
        PrivateUrl ossPrivateUrl = (PrivateUrl)super.clone();
        ossPrivateUrl.ossClient = new OSSClient(endpoint, new DefaultCredentialProvider(authKey1, authKey2), null);
        if (nextProcessor != null) ossPrivateUrl.nextProcessor = nextProcessor.clone();
        return ossPrivateUrl;
    }

    @Override
    public String resultInfo(Map<String, String> line) {
        return line.get("key");
    }

    @Override
    public boolean validCheck(Map<String, String> line) {
        return true;
    }

    @Override
    public void setNextProcessor(ILineProcess<Map<String, String>> nextProcessor) {
        this.nextProcessor = nextProcessor;
    }

    @Override
    public String singleResult(Map<String, String> line) throws IOException {
        String key = line.get("key");
        // 生成以GET方法访问的签名URL，访客可以直接通过浏览器访问相关内容。
        URL url = ossClient.generatePresignedUrl(bucket, key, expiration);
        if (nextProcessor != null) {
            line.put("url", url.toString());
            nextProcessor.processLine(line);
        }
        return url.toString();
    }

    @Override
    public void closeResource() {
        super.closeResource();
        endpoint = null;
        expiration = null;
        ossClient = null;
        nextProcessor = null;
    }
}
