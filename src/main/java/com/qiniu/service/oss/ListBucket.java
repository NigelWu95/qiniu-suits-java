package com.qiniu.service.oss;

import com.qiniu.common.*;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

public class ListBucket {

    private BucketManager bucketManager;

    public ListBucket(Auth auth, Configuration configuration) {
        this.bucketManager = new BucketManager(auth, configuration);
    }

    /*
    v2 的 list 接口，通过文本流的方式返回文件信息，v1 是单次请求的结果一次性返回。
     */
    public Response run(String bucket, String prefix, String delimiter, String marker, int limit, int retryCount, int version) throws QiniuException {

        Response response = null;
        try {
            response = version == 2 ?
                    bucketManager.listV2(bucket, prefix, marker, limit, delimiter) :
                    bucketManager.listV1(bucket, prefix, marker, limit, delimiter);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    System.out.println("listV" + version + " " + bucket + ":" + prefix + ":" + marker + ":" + limit + ":" + delimiter +
                            " " + e1.error() + ", last " + retryCount + " times retry...");
                    response = version == 2 ?
                            bucketManager.listV2(bucket, prefix, marker, limit, delimiter) :
                            bucketManager.listV1(bucket, prefix, marker, limit, delimiter);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }
}