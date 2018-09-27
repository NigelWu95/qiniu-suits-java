package com.qiniu.util;

import com.qiniu.common.QiniuException;
import com.qiniu.common.QiniuSuitsException;

public class HttpResponseUtils {

    public static int getNextRetryCount(QiniuException e, int retryCount) throws QiniuSuitsException {

        if (e.response == null || e.response.needRetry()) {
            retryCount--;
        } else {
            throw new QiniuSuitsException(e);
        }

        return retryCount;
    }

    public static void checkRetryCount(QiniuException e, int retryCount) throws QiniuSuitsException {

        if (e.response == null || e.response.needRetry()) {
            if (retryCount <= 0)
                throw new QiniuSuitsException(e);
        } else {
            throw new QiniuSuitsException(e);
        }
    }
}