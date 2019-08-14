package com.qiniu.process.aliyun;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.model.GeneratePresignedUrlRequest;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.process.Base;
import com.qiniu.util.CloudApiUtils;

import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.Map;

public class PrivateUrl extends Base<Map<String, String>> {

    private String endpoint;
    private Date expiration;
    private Map<String, String> queries;
    private GeneratePresignedUrlRequest request;
    private OSSClient ossClient;
    private ILineProcess<Map<String, String>> nextProcessor;

    public PrivateUrl(String accessKeyId, String accessKeySecret, String bucket, String endpoint, long expires,
                      Map<String, String> queries) {
        super("aliprivate", accessKeyId, accessKeySecret, bucket);
        this.endpoint = endpoint;
        expiration = new Date(System.currentTimeMillis() + expires);
        this.queries = queries;
        request = new GeneratePresignedUrlRequest(bucket, "");
        request.setExpiration(expiration);
        if (queries != null) {
            for (Map.Entry<String, String> entry : queries.entrySet())
                request.addQueryParameter(entry.getKey(), entry.getValue());
        }
        ossClient = new OSSClient(endpoint, new DefaultCredentialProvider(accessKeyId, accessKeySecret), null);
        CloudApiUtils.checkAliyun(ossClient);
    }

    public PrivateUrl(String accessKeyId, String accessKeySecret, String bucket, String endpoint, long expires,
                      Map<String, String> queries, String savePath, int saveIndex) throws IOException {
        super("aliprivate", accessKeyId, accessKeySecret, bucket, savePath, saveIndex);
        this.endpoint = endpoint;
        expiration = new Date(System.currentTimeMillis() + expires);
        this.queries = queries;
        request = new GeneratePresignedUrlRequest(bucket, "");
        request.setExpiration(expiration);
        if (queries != null) {
            for (Map.Entry<String, String> entry : queries.entrySet())
                request.addQueryParameter(entry.getKey(), entry.getValue());
        }
        ossClient = new OSSClient(endpoint, new DefaultCredentialProvider(accessKeyId, accessKeySecret), null);
        CloudApiUtils.checkAliyun(ossClient);
    }

    public PrivateUrl(String accessKeyId, String accessKeySecret, String bucket, String endpoint, long expires,
                      Map<String, String> queries, String savePath) throws IOException {
        this(accessKeyId, accessKeySecret, bucket, endpoint, expires, queries, savePath, 0);
    }

    public void setNextProcessor(ILineProcess<Map<String, String>> nextProcessor) {
        this.nextProcessor = nextProcessor;
        if (nextProcessor != null) processName = nextProcessor.getProcessName() + "_with_" + processName;
    }

    public PrivateUrl clone() throws CloneNotSupportedException {
        PrivateUrl ossPrivateUrl = (PrivateUrl)super.clone();
        ossPrivateUrl.request = new GeneratePresignedUrlRequest(bucket, "");
        ossPrivateUrl.request.setExpiration(expiration);
        if (queries != null) {
            for (Map.Entry<String, String> entry : queries.entrySet())
                ossPrivateUrl.request.addQueryParameter(entry.getKey(), entry.getValue());
        }
        ossPrivateUrl.ossClient = new OSSClient(endpoint, new DefaultCredentialProvider(accessId, secretKey), null);
        if (nextProcessor != null) ossPrivateUrl.nextProcessor = nextProcessor.clone();
        return ossPrivateUrl;
    }

    @Override
    public String resultInfo(Map<String, String> line) {
        return line.get("key");
    }

    @Override
    public String singleResult(Map<String, String> line) throws Exception {
        String key = line.get("key");
        if (key == null) throw new IOException("no key in " + line);
        request.setKey(key);
        URL url = ossClient.generatePresignedUrl(request);
        if (nextProcessor != null) {
            line.put("url", url.toString());
            return nextProcessor.processLine(line);
        }
        return key + "\t" + url.toString();
    }

    @Override
    public void closeResource() {
        super.closeResource();
        endpoint = null;
        expiration = null;
        queries = null;
        request = null;
        ossClient = null;
        if (nextProcessor != null) nextProcessor.closeResource();
        nextProcessor = null;
    }
}
