package com.qiniu.util;

import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class HttpResponseUtils {

    public static int getNextRetryCount(QiniuException e, int retryCount) throws QiniuException {
        if (e.response != null && e.response.needRetry()) {
            retryCount--;
            if (retryCount <= 0) throw e;
        } else {
            throw e;
        }

        return retryCount;
    }

    public static void checkRetryCount(QiniuException e, int retryCount) throws QiniuException {
        if (e.response != null && e.response.needRetry()) {
            if (retryCount <= 0) throw e;
        } else {
            throw e;
        }
    }

    public static void processException(QiniuException e, FileMap fileMap, List<String> infoList) throws IOException {
        // 取 error 信息从 exception 的 message 中取，避免 e.error() 抛出非预期异常
        if (e != null) {
            if (e.response != null) {
                if (fileMap != null) {
                    if (infoList == null || infoList.size() == 0)
                        fileMap.writeKeyFile("exception", e.response.reqId + "\t" + e.getMessage());
                    else
                        fileMap.writeKeyFile("exception", String.join("\n", infoList.stream()
                                .map(line -> line + "\t" + e.response.reqId + "\t" + e.getMessage())
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
                        fileMap.writeKeyFile("exception", e.getMessage());
                    else
                        fileMap.writeKeyFile("exception", String.join("\n", infoList.stream()
                                .map(line -> line + "\t" + e.getMessage())
                                .collect(Collectors.toList())));
                }
            }
        }
    }

    public static String getResult(Response response) throws QiniuException {
        if (response == null) return null;
        String responseBody = response.bodyString();
        if (response.statusCode != 200 && response.statusCode != 298) throw new QiniuException(response);
        response.close();
        return responseBody;
    }
}
