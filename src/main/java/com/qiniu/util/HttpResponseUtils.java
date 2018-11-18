package com.qiniu.util;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;

public class HttpResponseUtils {

    public static int getNextRetryCount(QiniuException e, int retryCount) throws QiniuException {

        if (e.response == null || e.response.needRetry()) {
            retryCount--;
            if (retryCount <= 0) throw e;
        } else {
            throw e;
        }

        return retryCount;
    }

    public static void checkRetryCount(QiniuException e, int retryCount) throws QiniuException {

        if (e.response == null || e.response.needRetry()) {
            if (retryCount <= 0) throw e;
        } else {
            throw e;
        }
    }

    public static void processException(QiniuException e, FileReaderAndWriterMap fileMap, String processName,
                                        String info) throws QiniuException {
        if (fileMap != null) fileMap.writeErrorOrNull(e.error() + "\t" + info);
        if (e == null) throw new QiniuException(null, processName + " failed.");
        if (e.response == null || !e.response.needRetry())
            throw new QiniuException(e, processName + " failed. " + e.error());
        else e.response.close();
    }
}
