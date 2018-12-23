package com.qiniu.util;

import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;

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

    public static void processException(QiniuException e, FileMap fileMap, String info)
            throws QiniuException {
        if (e != null) {
            if (e.response != null) {
                if (fileMap != null) fileMap.writeErrorOrNull(e.response.reqId + "\t" + info + "\t" + e.error());
                if (e.response.needSwitchServer() || e.response.statusCode == 631 || e.response.statusCode == 640) {
                    throw e;
                } else {
                    e.response.close();
                }
            } else {
                if (fileMap != null) fileMap.writeErrorOrNull( info + "\t" + e.error());
            }
        }
    }

    public static String getResult(Response response) throws QiniuException {
        if (response == null) return null;
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        if ((statusCode == -1) || (statusCode >= 300 && statusCode != 579))
            throw new QiniuException(response);
        response.close();
        return responseBody;
    }
}
