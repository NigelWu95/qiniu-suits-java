package com.qiniu.process.baidu;

import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import com.baidubce.services.bos.model.GeneratePresignedUrlRequest;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.process.Base;
import com.qiniu.util.CloudApiUtils;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

public class PrivateUrl extends Base<Map<String, String>> {

    private BosClientConfiguration configuration;
    private String endpoint;
    private int expires;
    private Map<String, String> queries;
    private GeneratePresignedUrlRequest request;
    private BosClient bosClient;
    private ILineProcess<Map<String, String>> nextProcessor;

    public PrivateUrl(String accessKeyId, String accessKeySecret, String bucket, String endpoint, int expires,
                      Map<String, String> queries) {
        super("baiduprivate", accessKeyId, accessKeySecret, bucket);
        this.endpoint = endpoint;
        this.expires = expires;
        this.queries = queries;
        request = new GeneratePresignedUrlRequest(bucket, "");
        request.setBucketName(bucket);
        request.setKey("");
        request.setExpiration(expires);
        if (queries != null) {
            for (Map.Entry<String, String> entry : queries.entrySet())
                request.addRequestParameter(entry.getKey(), entry.getValue());
        }
        configuration = new BosClientConfiguration().withEndpoint(endpoint).withCredentials(
                new DefaultBceCredentials(accessKeyId, accessKeySecret));
        bosClient = new BosClient(configuration);
        CloudApiUtils.checkBaidu(bosClient);
    }

    public PrivateUrl(String accessKeyId, String accessKeySecret, String bucket, String endpoint, int expires,
                      Map<String, String> queries, String savePath, int saveIndex) throws IOException {
        super("aliprivate", accessKeyId, accessKeySecret, bucket, savePath, saveIndex);
        this.endpoint = endpoint;
        this.expires = expires;
        this.queries = queries;
        request = new GeneratePresignedUrlRequest(bucket, "");
        request.setExpiration(expires);
        if (queries != null) {
            for (Map.Entry<String, String> entry : queries.entrySet())
                request.addRequestParameter(entry.getKey(), entry.getValue());
        }
        configuration = new BosClientConfiguration().withEndpoint(endpoint).withCredentials(
                new DefaultBceCredentials(accessKeyId, accessKeySecret));
        bosClient = new BosClient(configuration);
        CloudApiUtils.checkBaidu(bosClient);
    }

    public PrivateUrl(String accessKeyId, String accessKeySecret, String bucket, String endpoint, int expires,
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
        ossPrivateUrl.request.setExpiration(expires);
        if (queries != null) {
            for (Map.Entry<String, String> entry : queries.entrySet())
                ossPrivateUrl.request.addRequestParameter(entry.getKey(), entry.getValue());
        }
        ossPrivateUrl.bosClient = new BosClient(configuration);
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
        URL url = bosClient.generatePresignedUrl(request);
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
        queries = null;
        request = null;
        bosClient = null;
        if (nextProcessor != null) nextProcessor.closeResource();
        nextProcessor = null;
    }
}
