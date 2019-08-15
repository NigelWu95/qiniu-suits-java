package com.qiniu.process.tencent;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.model.GeneratePresignedUrlRequest;
import com.qcloud.cos.region.Region;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.process.Base;
import com.qiniu.util.CloudApiUtils;

import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.Map;

public class PrivateUrl extends Base<Map<String, String>> {

    private ClientConfig clientConfig;
    private GeneratePresignedUrlRequest request;
    private COSClient cosClient;
    private ILineProcess<Map<String, String>> nextProcessor;

    public PrivateUrl(String secretId, String secretKey, String bucket, String region, long expires, Map<String, String> queries) {
        super("tenprivate", secretId, secretKey, bucket);
        request = new GeneratePresignedUrlRequest(bucket, "");
        request.setExpiration(new Date(System.currentTimeMillis() + expires));
        if (queries != null) {
            for (Map.Entry<String, String> entry : queries.entrySet())
                request.addRequestParameter(entry.getKey(), entry.getValue());
        }
        clientConfig = new ClientConfig(new Region(region));
        cosClient = new COSClient(new BasicCOSCredentials(secretId, secretKey), clientConfig);
        CloudApiUtils.checkTencent(cosClient);
    }

    public PrivateUrl(String secretId, String secretKey, String bucket, String region, long expires, Map<String, String> queries,
                      String savePath, int saveIndex) throws IOException {
        super("tenprivate", secretId, secretKey, bucket, savePath, saveIndex);
        request = new GeneratePresignedUrlRequest(bucket, "");
        request.setExpiration(new Date(System.currentTimeMillis() + expires));
        if (queries != null) {
            for (Map.Entry<String, String> entry : queries.entrySet())
                request.addRequestParameter(entry.getKey(), entry.getValue());
        }
        clientConfig = new ClientConfig(new Region(region));
        cosClient = new COSClient(new BasicCOSCredentials(secretId, secretKey), clientConfig);
        CloudApiUtils.checkTencent(cosClient);
    }

    public PrivateUrl(String secretId, String secretKey, String bucket, String endpoint, long expires, Map<String, String> queries,
                      String savePath) throws IOException {
        this(secretId, secretKey, bucket, endpoint, expires, queries, savePath, 0);
    }

    public void setNextProcessor(ILineProcess<Map<String, String>> nextProcessor) {
        this.nextProcessor = nextProcessor;
        if (nextProcessor != null) processName = nextProcessor.getProcessName() + "_with_" + processName;
    }

    public PrivateUrl clone() throws CloneNotSupportedException {
        PrivateUrl cosPrivateUrl = (PrivateUrl)super.clone();
        cosPrivateUrl.request = (GeneratePresignedUrlRequest) request.clone();
        cosPrivateUrl.cosClient = new COSClient(new BasicCOSCredentials(accessId, secretKey), clientConfig);
        if (nextProcessor != null) cosPrivateUrl.nextProcessor = nextProcessor.clone();
        return cosPrivateUrl;
    }

    @Override
    public String resultInfo(Map<String, String> line) {
        return line.get("key");
    }

    @Override
    public String singleResult(Map<String, String> line) throws Exception {
        String key = line.get("key");
        if (key == null) throw new IOException("key is not exists or empty in " + line);
        request.setKey(key);
        URL url = cosClient.generatePresignedUrl(request);
        if (nextProcessor != null) {
            line.put("url", url.toString());
            return nextProcessor.processLine(line);
        }
        return key + "\t" + url.toString();
    }

    @Override
    public void closeResource() {
        super.closeResource();
        clientConfig = null;
        request = null;
        cosClient.shutdown();
        cosClient = null;
        if (nextProcessor != null) nextProcessor.closeResource();
        nextProcessor = null;
    }
}
