package com.qiniu.service.oss;

import com.google.gson.Gson;
import com.qiniu.sdk.QiniuAuth;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.StringMap;

import java.util.HashMap;
import java.util.Map;

public class AsyncFetch {

    private QiniuAuth auth;
    private Client client;
    private String bucket;

    private static volatile AsyncFetch asyncFetch = null;

    public AsyncFetch(QiniuAuth auth, String bucket) {
        this.bucket = bucket;
        this.auth = auth;
        this.client = new Client();
    }

    public static AsyncFetch getInstance(QiniuAuth auth, String bucket) {
        if (asyncFetch == null) {
            synchronized (AsyncFetch.class) {
                if (asyncFetch == null) {
                    asyncFetch = new AsyncFetch(auth, bucket);
                }
            }
        }
        return asyncFetch;
    }

    public String run(String url, String key, int retryCount) throws QiniuException {

        Response response = asyncFetchWithRetry(url, key, retryCount);
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        response.close();

        return statusCode + "\t" + reqId + "\t" + responseBody;
    }

    public Response asyncFetchWithRetry(String url, String key, int retryCount) throws QiniuException {

        Response response = null;
        Gson gson = new Gson();
        Map<String, String> bodyMap = new HashMap<>();
        bodyMap.put("url", url);
        bodyMap.put("bucket", bucket);
        bodyMap.put("key", key);
        String jsonBody = gson.toJson(bodyMap);
        byte[] bodyBytes = jsonBody.getBytes();
        String apiUrl = "http://api.qiniu.com/sisyphus/fetch";
        String accessToken = "Qiniu " + auth.signRequestV2(apiUrl, "POST", bodyBytes, "application/json");
        StringMap headers = new StringMap();
        headers.put("Authorization", accessToken);

        try {
            response = client.post(apiUrl, bodyBytes, headers, Client.JsonMime);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    System.out.println(url + "\t" + e1.error() + ", last " + retryCount + " times retry...");
                    response = client.post(apiUrl, bodyBytes, headers, Client.JsonMime);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }
}