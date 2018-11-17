package com.qiniu.service.oss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.model.FetchBody;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;

public class AsyncFetch extends OperationBase implements Cloneable {

    public AsyncFetch(Auth auth, Configuration configuration) {
        super(auth, configuration);
    }

    public AsyncFetch clone() throws CloneNotSupportedException {
        return (AsyncFetch) super.clone();
    }

    public String run(FetchBody fetchBody, boolean keepKey, String prefix, int retryCount) throws QiniuException {

        Response response = fetchWithRetry(fetchBody, keepKey, prefix, retryCount);
        if (response == null) return null;
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        response.close();

        return statusCode + "\t" + reqId + "\t" + responseBody;
    }

    public Response fetchWithRetry(FetchBody fetchBody, boolean keepKey, String prefix, int retryCount)
            throws QiniuException {

        Response response = null;
        try {
            response = bucketManager.asynFetch(fetchBody.url, fetchBody.bucket, fetchBody.key);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    System.out.println("async fetch " + fetchBody.url + " to " + fetchBody.bucket + ":" + fetchBody.key
                            + " " + e1.error() + ", last " + retryCount + " times retry...");
                    response = bucketManager.asynFetch(fetchBody.url, fetchBody.bucket, fetchBody.key);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }
}