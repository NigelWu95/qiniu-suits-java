package com.qiniu.service.oss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.model.FetchBody;
import com.qiniu.model.FetchFile;
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
        FetchFile fetchFile = fetchBody.fetchFiles.get(0);
        try {
            response = fetchBody.hasCustomArgs() ?
                    bucketManager.asynFetch(fetchFile.url, fetchBody.bucket, fetchFile.key, fetchFile.md5,
                            fetchFile.etag, fetchBody.callbackUrl, fetchBody.callbackBody, fetchBody.callbackBodyType,
                            fetchBody.callbackHost, fetchBody.fileType) :
                    bucketManager.asynFetch(fetchFile.url, fetchBody.bucket, fetchFile.key);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    System.out.println("async fetch " + fetchFile.url + " to " + fetchBody.bucket + ":" + fetchFile.key
                            + " " + e1.error() + ", last " + retryCount + " times retry...");
                    response = bucketManager.asynFetch(fetchFile.url, fetchBody.bucket, fetchFile.key);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }
}
