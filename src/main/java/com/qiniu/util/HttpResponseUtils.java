package com.qiniu.util;

import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

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

    public static void processException(QiniuException e, FileMap fileMap, List<String> infoList) throws IOException {
        if (e != null) {
            if (e.response != null) {
                if (fileMap != null) {
                    if (infoList == null || infoList.size() == 0)
                        fileMap.writeError(e.response.reqId + "\t" + e.error());
                    else
                        fileMap.writeError(String.join("\n", infoList.stream()
                                .map(line -> e.response.reqId + "\t" + line + "\t" + e.error())
                                .collect(Collectors.toList())));
                }
                if (e.response.needSwitchServer() || e.response.statusCode >= 630) {
                    throw e;
                } else {
                    e.response.close();
                }
            } else {
                if (fileMap != null) {
                    if (infoList == null || infoList.size() == 0)
                        fileMap.writeError(e.error());
                    else
                        fileMap.writeError(String.join("\n", infoList.stream()
                                .map(line -> line + "\t" + e.error())
                                .collect(Collectors.toList())));
                }
            }
        }
    }

    public static String getResult(Response response) throws QiniuException {
        if (response == null) return null;
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        if (statusCode != 200 && statusCode != 298) throw new QiniuException(response);
        response.close();
        return responseBody;
    }
}
