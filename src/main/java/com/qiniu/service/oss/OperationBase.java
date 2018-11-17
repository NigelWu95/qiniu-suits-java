package com.qiniu.service.oss;

import com.qiniu.sdk.BucketManager;
import com.qiniu.sdk.BucketManager.*;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;

public abstract class OperationBase {

    protected Auth auth;
    protected Configuration configuration;
    protected BucketManager bucketManager;
    protected volatile BatchOperations batchOperations;

    public OperationBase(Auth auth, Configuration configuration) {
        this.auth = auth;
        this.configuration = configuration;
        this.bucketManager = new BucketManager(auth, configuration);
        this.batchOperations = new BatchOperations();
    }

    public OperationBase clone() throws CloneNotSupportedException {
        OperationBase operationBase = (OperationBase)super.clone();
        operationBase.bucketManager = new BucketManager(auth, configuration);
        operationBase.batchOperations = new BatchOperations();
        return operationBase;
    }

    public Response batchWithRetry(int retryCount) throws QiniuException {
        Response response = null;

        try {
            response = bucketManager.batch(batchOperations);
        } catch (QiniuException e) {
            HttpResponseUtils.checkRetryCount(e, retryCount);
            while (retryCount > 0) {
                try {
                    response = bucketManager.batch(batchOperations);
                    retryCount = 0;
                } catch (QiniuException e1) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e1, retryCount);
                }
            }
        }

        return response;
    }
}
