package com.qiniu.process.huawei;

import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.obs.services.model.TemporarySignatureRequest;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.process.Base;
import com.qiniu.util.CloudApiUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PrivateUrl extends Base<Map<String, String>> {

    private ObsConfiguration configuration;
    private long expires;
    private Map<String, Object> queries;
    private TemporarySignatureRequest request;
    private ObsClient obsClient;
    private ILineProcess<Map<String, String>> nextProcessor;

    public PrivateUrl(String accessKeyId, String accessKeySecret, String bucket, String endpoint, long expires,
                      Map<String, String> queries) {
        super("huaweiprivate", accessKeyId, accessKeySecret, bucket);
        this.expires = expires;
        this.queries = new HashMap<>();
        request = new TemporarySignatureRequest();
        request.setBucketName(bucket);
        request.setObjectKey("");
        request.setExpires(expires);
        if (queries != null) {
            for (Map.Entry<String, String> entry : queries.entrySet())
                queries.put(entry.getKey(), entry.getValue());
        }
        request.setQueryParams(this.queries);
        configuration = new ObsConfiguration();
        configuration.setEndPoint(endpoint);
        obsClient = new ObsClient(accessId, secretKey, configuration);
        CloudApiUtils.checkHuaWei(obsClient);
    }

    public PrivateUrl(String accessKeyId, String accessKeySecret, String bucket, String endpoint, long expires,
                      Map<String, String> queries, String savePath, int saveIndex) throws IOException {
        super("huaweiprivate", accessKeyId, accessKeySecret, bucket, savePath, saveIndex);
        this.expires = expires;
        this.queries = new HashMap<>();
        request = new TemporarySignatureRequest();
        request.setBucketName(bucket);
        request.setObjectKey("");
        request.setExpires(expires);
        if (queries != null) {
            for (Map.Entry<String, String> entry : queries.entrySet())
                queries.put(entry.getKey(), entry.getValue());
        }
        request.setQueryParams(this.queries);
        configuration = new ObsConfiguration();
        configuration.setEndPoint(endpoint);
        obsClient = new ObsClient(accessId, secretKey, configuration);
        CloudApiUtils.checkHuaWei(obsClient);
    }

    public PrivateUrl(String accessKeyId, String accessKeySecret, String bucket, String endpoint, long expires,
                      Map<String, String> queries, String savePath) throws IOException {
        this(accessKeyId, accessKeySecret, bucket, endpoint, expires, queries, savePath, 0);
    }

    public void setNextProcessor(ILineProcess<Map<String, String>> nextProcessor) {
        this.nextProcessor = nextProcessor;
        if (nextProcessor != null) processName = String.join("_with_", nextProcessor.getProcessName(), processName);
    }

    public PrivateUrl clone() throws CloneNotSupportedException {
        PrivateUrl ossPrivateUrl = (PrivateUrl)super.clone();
        ossPrivateUrl.queries = new HashMap<>(queries);
        ossPrivateUrl.request = new TemporarySignatureRequest();
        ossPrivateUrl.request.setBucketName(bucket);
        ossPrivateUrl.request.setObjectKey("");
        ossPrivateUrl.request.setExpires(expires);
        ossPrivateUrl.obsClient = new ObsClient(accessId, secretKey, configuration);
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
        request.setObjectKey(key);
        String url = obsClient.createTemporarySignature(request).getSignedUrl();
        if (nextProcessor != null) {
            line.put("url", url);
            return nextProcessor.processLine(line);
        }
        return String.join("\t", key, url);
    }

    @Override
    public void closeResource() {
        super.closeResource();
        configuration = null;
        queries = null;
        request = null;
        if (obsClient != null) {
            try {
                obsClient.close();
            } catch (IOException ignored) {
                // 有关闭异常直接避开
            }
            obsClient = null;
        }
        if (nextProcessor != null) nextProcessor.closeResource();
        nextProcessor = null;
    }
}
