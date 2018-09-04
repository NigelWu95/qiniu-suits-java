package com.qiniu.service.oss;
import com.google.gson.Gson;
import com.qiniu.common.QiniuSuitsException;
import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.util.Auth;
import com.qiniu.util.Json;
import com.qiniu.util.StringMap;

import java.util.HashMap;
import java.util.Map;

public class AsyncFetchProcessor {

    private QiniuAuth auth;
    private Client client;
    private Response response;
    private String bucket;

    private static volatile AsyncFetchProcessor asyncFetchProcessor = null;

    private AsyncFetchProcessor(QiniuAuth auth, String bucket) {
        this.bucket = bucket;
        this.auth = auth;
        this.client = new Client();
    }

    public static AsyncFetchProcessor getAsyncFetchProcessor(QiniuAuth auth, String bucket) throws QiniuSuitsException {
        if (asyncFetchProcessor == null) {
            synchronized (AsyncFetchProcessor.class) {
                if (asyncFetchProcessor == null) {
                    asyncFetchProcessor = new AsyncFetchProcessor(auth, bucket);
                }
            }
        }
        return asyncFetchProcessor;
    }

    public String doAsyncFetch(String url, String key) throws QiniuSuitsException {
        // 构造post请求body
        Gson gson = new Gson();
        Map<String, String> bodyMap = new HashMap();
        bodyMap.put("url", url);
        bodyMap.put("bucket", bucket);
        bodyMap.put("key", key);
        String jsonBody = gson.toJson(bodyMap);
        byte[] bodyBytes = jsonBody.getBytes();
        // 获取签名
        String apiUrl = "http://api.qiniu.com/sisyphus/fetch";
        String accessToken = "Qiniu " + auth.signRequestV2(apiUrl, "POST", bodyBytes, "application/json");
        StringMap headers = new StringMap();
        headers.put("Authorization", accessToken);
        String respBody = "";

        try {
            response = client.post(apiUrl, bodyBytes, headers, Client.JsonMime);
        } catch (QiniuException e) {
            QiniuSuitsException qiniuSuitsException = new QiniuSuitsException(e);
            qiniuSuitsException.addToFieldMap("url", url);
            qiniuSuitsException.addToFieldMap("key", key);
            qiniuSuitsException.setStackTrace(e.getStackTrace());
            throw qiniuSuitsException;
        }

        int statusCode = response.statusCode;
        String reqId = response.reqId;

        try {
            respBody = response.bodyString();
        } catch (QiniuException qiniuException) {
            statusCode = 0;
        }

        if (statusCode == 200) {
            return respBody.replaceAll("id\":\"\\w", "reqid\":\"" + reqId);
        } else {
            QiniuSuitsException qiniuSuitsException = new QiniuSuitsException("async fetch error");
            qiniuSuitsException.addToFieldMap("statusCode", String.valueOf(statusCode));
            qiniuSuitsException.addToFieldMap("reqId", reqId);
            qiniuSuitsException.addToFieldMap("respBody", respBody);
            throw qiniuSuitsException;
        }
    }

    public void closeClient() {
        if (response != null) {
            response.close();
        }
    }
}