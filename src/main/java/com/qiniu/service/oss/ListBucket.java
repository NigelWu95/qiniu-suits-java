package com.qiniu.service.oss;

import com.qiniu.common.*;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

public class ListBucket {

    private QiniuAuth auth;
    private Configuration configuration;
    private QiniuBucketManager bucketManager;
    private Client client;

    public ListBucket(QiniuAuth auth, Configuration configuration) {
        this.auth = auth;
        this.configuration = configuration;
        this.bucketManager = new QiniuBucketManager(auth, configuration);
        this.client = new Client();
    }

    public Response run(String bucket, String prefix, String delimiter, String marker, int limit, int retryCount, int version) throws QiniuException {

        return version == 2 ?
                    listV2WithRetry(bucket, prefix, delimiter, marker, limit, retryCount) :
                    listV1WithRetry(bucket, prefix, delimiter, marker, limit, retryCount);
    }

    public Response listV1WithRetry(String bucket, String prefix, String delimiter, String marker, int limit, int retryCount) throws QiniuException {

        Response response = null;
        StringMap map = new StringMap().put("bucket", bucket).putNotEmpty("prefix", prefix).putNotEmpty("delimiter", delimiter)
                .putNotEmpty("marker", marker).putWhen("limit", limit, limit > 0);
        String url = String.format("%s/list?%s", configuration.rsfHost(auth.accessKey, bucket), map.formString());
        StringMap headers = auth.authorization(url);

        try {
            response = client.get(url, headers);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    System.out.println(url + "\t" + e1.error() + ", last " + retryCount + " times retry...");
                    response = client.get(url, headers);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }

    /*
    v2 的 list 接口，通过文本流的方式返回文件信息。
     */
    public Response listV2WithRetry(String bucket, String prefix, String delimiter, String marker, int limit, int retryCount) throws QiniuException {

        Response response = null;
        StringMap map = new StringMap().put("bucket", bucket).putNotEmpty("prefix", prefix).putNotEmpty("delimiter", delimiter)
                .putNotEmpty("marker", marker).putWhen("limit", limit, limit > 0);
        String url = String.format("http://rsf.qbox.me/v2/list?%s", map.formString());
        String authorization = "QBox " + auth.signRequest(url, null, null);
        StringMap headers = new StringMap().put("Authorization", authorization);

        try {
            response = client.post(url, null, headers, null);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    System.out.println(url + "\t" + e1.error() + ", last " + retryCount + " times retry...");
                    response = client.post(url, null, headers, null);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }

    public void closeBucketManager() {
        if (bucketManager != null)
            bucketManager.closeResponse();
    }
}