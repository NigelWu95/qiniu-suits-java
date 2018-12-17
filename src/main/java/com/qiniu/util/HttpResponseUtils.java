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

    public static void processException(QiniuException e, FileMap fileMap, String processName, String info)
            throws QiniuException {
        if (e != null) {
            if (fileMap != null) fileMap.writeErrorOrNull(processName + " failed. " + e.error() + "\t" + info);
            if (e.response != null) {
                if (e.response.needSwitchServer() || e.response.statusCode == 631 || e.response.statusCode == 640) {
                    throw e;
                } else {
                    e.response.close();
                }
            }
        }
    }

    public static String getResult(Response response) throws QiniuException {
        if (response == null) return null;
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        if (statusCode != 200) throw new QiniuException(response);
        String reqId = response.reqId;
        response.close();
        return statusCode + "\t" + reqId + "\t" + responseBody;
    }
}
