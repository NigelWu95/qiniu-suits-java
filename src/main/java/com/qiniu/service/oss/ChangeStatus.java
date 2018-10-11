package com.qiniu.service.oss;

import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuBucketManager;
import com.qiniu.common.QiniuBucketManager.*;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.util.HttpResponseUtils;

import java.util.ArrayList;

public class ChangeStatus implements Cloneable {

    private QiniuAuth auth;
    private Configuration configuration;
    private Client client;
    private QiniuBucketManager bucketManager;
    private BatchOperations batchOperations;

    public ChangeStatus(QiniuAuth auth, Configuration configuration) {
        this.auth = auth;
        this.configuration = configuration;
        this.client = new Client(configuration);
        this.bucketManager = new QiniuBucketManager(auth, configuration);
        this.batchOperations = new BatchOperations();
    }

    public ChangeStatus clone() throws CloneNotSupportedException {
        ChangeStatus changeStatus = (ChangeStatus)super.clone();
        changeStatus.client = new Client(configuration);
        changeStatus.bucketManager = new QiniuBucketManager(auth, configuration);
        changeStatus.batchOperations = new BatchOperations();
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

    public ArrayList<String> getBatchOps() {
        return batchOperations.getOps();
    }

    public String batchRun(String bucket, String key, short status, int retryCount) throws QiniuException {
        Response response = batchChangeStatusWithRetry(bucket, key, status, retryCount);
        if (response == null) return null;
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        response.close();

        return statusCode + "\t" + reqId + "\t" + responseBody;
    }

    public Response changeStatusWithRetry(String bucket, String key, short status, int retryCount) throws QiniuException {

        Response response = null;
//        String url = "http://rs.qiniu.com/chstatus/" + UrlSafeBase64.encodeToString(bucket + ":" + key) + "/status/" + status;
//        String accessToken = "QBox " + auth.signRequest(url, null, Client.FormMime);
//        StringMap headers = new StringMap();
//        headers.put("Authorization", accessToken);
//
//        try {
//            response = client.post(url, null, headers, Client.FormMime);
//        } catch (QiniuException e1) {
//            HttpResponseUtils.checkRetryCount(e1, retryCount);
//            while (retryCount > 0) {
//                try {
//                    System.out.println(url + "\t" + e1.error() + ", last " + retryCount + " times retry...");
//                    response = client.post(url, null, headers, Client.FormMime);
//                    retryCount = 0;
//                } catch (QiniuException e2) {
//                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
//                }
//            }
//        }

        try {
            response = bucketManager.changeStatus(bucket, key, status);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    System.out.println("status " + bucket + ":" + key + " to " + status + " " + e1.error() + ", last "
                            + retryCount + " times retry...");
                    response = bucketManager.changeStatus(bucket, key, status);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }

    public Response batchChangeStatusWithRetry(String bucket, String key, short status, int retryCount) throws QiniuException {
        Response response = null;

        try {
            if (batchOperations.getOps().size() < 1000) batchOperations.addChangeStatusOps(bucket, status, key);
            else response = bucketManager.batch(batchOperations);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    System.out.println("status " + bucket + ":" + key + " to " + status + " " + e1.error() + ", last "
                            + retryCount + " times retry...");
                    response = bucketManager.batch(batchOperations);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }
        batchOperations.clearOps();
        return response;
    }
}