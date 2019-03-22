package com.qiniu.util;

import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class HttpResponseUtils {

    /**
     * 取出异常中的信息
     * @param e 异常对象，不能为空
     * @return 信息字符串
     */
    private static String getMessage(QiniuException e) {
        // 取 error 信息优先从 exception 的 message 中取，避免直接 e.error() 抛出非预期异常，因为 error() 方法底层可能会调用
        // response.isJson()，该方法底层会抛出空指针异常（位置 com.qiniu.http.Response.ctype(Response.java:137)），同时
        // getMessage 会包含 reqid 等信息
        String message = e.getMessage() == null ? "" : "message:" + e.getMessage();
        if ("".equals(message)) {
            message = (e.response != null ? "reqid:" + e.response.reqId + ", ": "") + "code:" + e.code();
            // 避免抛出空指针异常
            try { message += "， error:" + e.error(); } catch (Exception ex) { ex.printStackTrace(); }
        }
        return message;
    }

    /**
     * 从异常中提取错误描述进行记录，并一一对应输入行信息
     * @param e 需要记录异常的 QiniuException 对象
     * @param fileMap 记录错误信息的持久化对象
     * @param info 需要和错误信息同时记录的原始 info（可以传入文件名等信息）
     * @throws IOException 持久化错误信息失败可能抛出的异常
     */
    public static void writeLog(QiniuException e, FileMap fileMap, String info) throws IOException {
        if (fileMap == null) return;
        String message = e != null ? getMessage(e) : "";
        fileMap.writeError(info + "\t" + message.replaceAll("\n", "\t"), false);
    }

    /**
     * 从异常中提取错误描述进行记录，并一一对应输入行信息
     * @param e 需要记录异常的 QiniuException 对象
     * @param fileMap 记录错误信息的持久化对象
     * @param infoList 需要和错误信息同时记录的原始 info 列表
     * @throws IOException 持久化错误信息失败可能抛出的异常
     */
    public static void writeLog(QiniuException e, FileMap fileMap, List<String> infoList) throws IOException {
        if (fileMap == null) return;
        String message = e != null ? getMessage(e) : "";
        if (infoList == null || infoList.size() == 0) {
            fileMap.writeError(message.replaceAll("\n", "\t"), false);
        } else {
            fileMap.writeError(String.join("\n", infoList.stream()
                    .map(line -> line + "\t" + message.replaceAll("\n", "\t"))
                    .collect(Collectors.toList())), false);
        }
    }

    /**
     * 处理异常结果，提取异常信息进行判断或者在需要抛出异常时记录具体错误描述
     * @param e 需要处理的 QiniuException 异常
     * @param retry 当前重试次数
     * @param fileMap 记录错误信息的持久化对象
     * @param infoList 需要和错误信息同时记录的原始 info 列表
     * @return 返回重试次数（可能有些异常需要直接将重试次数置为 0 但不抛出异常）
     * @throws IOException 持久化错误信息失败可能抛出的异常或传入的异常经判断后需要抛出
     */
    public static int processException(QiniuException e, int retry, FileMap fileMap, List<String> infoList)
            throws IOException {
        if (e != null) {
            System.out.println("code:" + e.code() + ", message:" + e.getMessage());
            if (e.response != null) {
                // 478 状态码表示镜像源返回了非 200 的状态码，避免因为该异常导致程序终端先处理该异常
                if (e.code() == 478 || e.code() == 404) {
                    return 0;
                }
                // 631 状态码表示空间不存在，则不需要重试直接走抛出异常方式
                else if (e.code() != 631 && e.response.needRetry() && retry > 0) {
                    // 可重试的异常信息不需要记录，因为重试之后可能成功或者再次进行该方法
//                    e.printStackTrace();
                    e.response.close();
                } else {
                    // 需要抛出异常时将错误信息记录下来
                    for (String info : infoList) {
                        writeLog(e, fileMap, info);
                    }
                    throw e;
                }
            } else {
                if (retry <= 0) {
                    // 没有重试机会时将错误信息记录下来
                    for (String info : infoList) {
                        writeLog(e, fileMap, info);
                    }
                    throw e;
                } else {
                    // 重试次数大于 0 时只输出错误信息，不需要记录，因为重试之后可能成功或者再次进行该方法
                    e.printStackTrace();
                }
            }
        }

        // 处理一次异常返回的重试次数应该少一次
        return retry - 1;
    }

    /**
     * 处理异常结果，提取异常信息进行判断或者在需要抛出异常时记录具体错误描述
     * @param e 需要处理的 QiniuException 异常
     * @param retry 当前重试次数
     * @return 返回重试次数，返回 -2 表示传入的异常为空，返回 -1 表示该异常应该抛出，返回 0 表示该异常可以记录并跳过，返回 1 表示可以进行重试
     */
    public static int checkException(QiniuException e, int retry) {
        if (e != null) {
            System.out.println("code:" + e.code() + ", message:" + e.getMessage());
            if (e.response != null) {
                // 478 状态码表示镜像源返回了非 200 的状态码，避免因为该异常导致程序中断先处理该异常
                if (e.code() == 478 || e.code() == 404) {
                    return 0;
                }
                // 631 状态码表示空间不存在，则不需要重试直接走抛出异常方式
                else if (e.code() != 631 && e.response.needRetry() && retry > 0) {
                    e.response.close();
                } else {
                    return -1;
                }
            } else {
                if (retry <= 0) {
                    return -1;
                }
            }
        } else {
            return -2;
        }

        // 处理一次异常返回的重试次数应该少一次
        return retry - 1;
    }

    /**
     * 将 Response 对象转换成为结果字符串
     * @param response 得到的 Response 对象
     * @return Response body 转换的 String 对象
     * @throws QiniuException Response 非正常响应的情况下抛出的异常
     */
    public static String getResult(Response response) throws QiniuException {
        if (response == null) throw new QiniuException(new Exception("empty response"));
        if (response.statusCode != 200 && response.statusCode != 298) throw new QiniuException(response);
        String responseBody = response.bodyString();
        response.close();
        return responseBody;
    }

    /**
     * 将 Response 对象转换成为 json 格式结果字符串
     * @param response 得到的 Response 对象
     * @return Response body 转换的 String 对象，用 json 格式记录，包括 status code
     * @throws QiniuException Response 非正常响应的情况下抛出的异常
     */
    public static String responseJson(Response response) throws QiniuException {
        String result = getResult(response);
        return "{\"code\":" + response.statusCode + ",\"message\":\"" + result + "\"}";
    }
}
