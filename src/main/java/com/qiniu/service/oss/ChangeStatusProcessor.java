package com.qiniu.service.oss;

import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuBucketManager;
import com.qiniu.common.QiniuException;
import com.qiniu.common.QiniuSuitsException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.StringMap;
import com.qiniu.util.UrlSafeBase64;

public class ChangeStatusProcessor {

    private QiniuAuth auth;
    private Client client;

    private static volatile ChangeStatusProcessor changeStatusProcessor = null;

    public ChangeStatusProcessor(QiniuAuth auth, Client client) {
        this.auth = auth;
        this.client = client;
    }

    public static ChangeStatusProcessor getChangeStatusProcessor(QiniuAuth auth, Client client) {
        if (changeStatusProcessor == null) {
            synchronized (ChangeStatusProcessor.class) {
                if (changeStatusProcessor == null) {
                    changeStatusProcessor = new ChangeStatusProcessor(auth, client);
                }
            }
        }
        return changeStatusProcessor;
    }

    public String doStatusChange(String bucket, String key, short status) throws QiniuSuitsException {

        return doStatusChange(bucket, key, status, 0);
    }

    public String doStatusChange(String bucket, String key, short status, int retryCount) throws QiniuSuitsException {

        Response response = null;
        String responseBody;
        int statusCode;
        String reqId;

        try {
            response = changeStatusWithRetry(bucket, key, status, retryCount);
            responseBody = response.bodyString();
            statusCode = response.statusCode;
            reqId = response.reqId;
        } catch (QiniuException e) {
            QiniuSuitsException qiniuSuitsException = new QiniuSuitsException("change file status error");
            qiniuSuitsException.addToFieldMap("code", String.valueOf(e.code()));
            qiniuSuitsException.addToFieldMap("error", String.valueOf(e.error()));
            qiniuSuitsException.setStackTrace(e.getStackTrace());
            throw qiniuSuitsException;
        } finally {
            if (response != null)
                response.close();
        }

        return statusCode + "\t" + reqId + "\t" + responseBody;
    }

    private Response changeStatusWithRetry(String bucket, String key, short status, int retryCount) throws QiniuSuitsException {

        Response response = null;
        String url = "http://rs.qiniu.com/chstatus/" + UrlSafeBase64.encodeToString(bucket + ":" + key) + "/status/" + status;
        String accessToken = "QBox " + auth.signRequest(url, null, Client.FormMime);
        StringMap headers = new StringMap();
        headers.put("Authorization", accessToken);

        try {
            response = client.post(url, null, headers, Client.FormMime);
        } catch (QiniuException e1) {
            if (retryCount <= 0) {
                throw new QiniuSuitsException(e1);
            }
            while (retryCount > 0) {
                try {
                    System.out.println(e1.getMessage() + ", last " + retryCount + " times retry...");
                    response = client.post(url, null, headers, Client.FormMime);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }
}