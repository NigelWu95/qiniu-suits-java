package com.qiniu.util;

import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class HttpResponseUtils {

    public static void writeLog(QiniuException e, FileMap fileMap, List<String> infoList) throws IOException {
        if (fileMap == null) return;
        String message = "";
        if (e != null) {
            message = e.getMessage() == null ? "" : e.getMessage();
            if ("".equals(message)) {
                message = e.response != null ? e.response.reqId + "\t" : "";
                try {
                    message += e.error() == null ? "" : e.error();
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
        }
        if (infoList == null || infoList.size() == 0) {
            fileMap.writeError(message.replaceAll("\n", "\t"), false);
        } else {
            String finalMessage = message;
            fileMap.writeError(String.join("\n", infoList.stream()
                    .map(line -> line + "\t" + finalMessage.replaceAll("\n", "\t"))
                    .collect(Collectors.toList())), false);
        }
    }

    public static void processException(QiniuException e, int retry, FileMap fileMap, List<String> infoList)
            throws IOException {
        // 取 error 信息优先从 exception 的 message 中取，避免直接调用 e.error() 抛出非预期异常，同时 getMessage 包含 reqid 等信息
        if (e != null) {
            if (e.response != null) {
                // 需要抛出异常时将错误信息记录下来
                if (retry <= 0 || e.response.statusCode == 631 || !e.response.needRetry()) {
                    writeLog(e, fileMap, infoList);
                    throw e;
                } else {
                    e.printStackTrace();
                    e.response.close();
                }
            } else {
                // 没有重试机会时将错误信息记录下来
                if (retry <= 0) {
                    writeLog(e, fileMap, infoList);
                    throw e;
                } else {
                    e.printStackTrace();
                }
            }
        }
    }

    public static String getResult(Response response) throws QiniuException {
        if (response == null) throw new QiniuException(new Exception("empty response"));
        if (response.statusCode != 200 && response.statusCode != 298) throw new QiniuException(response);
        String responseBody = response.bodyString();
        response.close();
        return responseBody;
    }

    public static String responseJson(Response response) throws QiniuException {
        String result = getResult(response);
        return "{\"code\":" + response.statusCode + ",\"message\":\"" + result + "\"}";
    }
}
