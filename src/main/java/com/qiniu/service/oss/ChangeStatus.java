package com.qiniu.service.oss;

import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.StringMap;
import com.qiniu.util.UrlSafeBase64;

public class ChangeStatus {

    private QiniuAuth auth;
    private Client client;

    private static volatile ChangeStatus changeStatus = null;

    public ChangeStatus(QiniuAuth auth, Client client) {
        this.auth = auth;
        this.client = client;
    }

    public static ChangeStatus getInstance(QiniuAuth auth, Client client) {
        if (changeStatus == null) {
            synchronized (ChangeStatus.class) {
                if (changeStatus == null) {
                    changeStatus = new ChangeStatus(auth, client);
                }
            }
        }
        return changeStatus;
    }

    public String run(String bucket, String key, short status, int retryCount) throws QiniuException {

        Response response = changeStatusWithRetry(bucket, key, status, retryCount);
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        response.close();

        return statusCode + "\t" + reqId + "\t" + responseBody;
    }

    public Response changeStatusWithRetry(String bucket, String key, short status, int retryCount) throws QiniuException {

        Response response = null;
        String url = "http://rs.qiniu.com/chstatus/" + UrlSafeBase64.encodeToString(bucket + ":" + key) + "/status/" + status;
        String accessToken = "QBox " + auth.signRequest(url, null, Client.FormMime);
        StringMap headers = new StringMap();
        headers.put("Authorization", accessToken);

        try {
            response = client.post(url, null, headers, Client.FormMime);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
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