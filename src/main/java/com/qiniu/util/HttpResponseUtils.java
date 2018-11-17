package com.qiniu.util;

import com.qiniu.common.QiniuException;

public class HttpResponseUtils {

    public static int getNextRetryCount(QiniuException e, int retryCount) throws QiniuException {

        if (e.response == null || e.response.needRetry()) {
            retryCount--;
        } else {
            throw e;
        }

        return retryCount;
    }

    public static void checkRetryCount(QiniuException e, int retryCount) throws QiniuException {

        if (e.response == null || e.response.needRetry()) {
            if (retryCount <= 0)
                throw e;
        } else {
            throw e;
        }
    }
}
