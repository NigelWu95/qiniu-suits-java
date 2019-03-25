package com.qiniu.util;

import com.qiniu.common.QiniuException;
import com.qiniu.persistence.FileMap;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class LogUtils {

    /**
     * 取出异常中的信息
     * @param e 异常对象，不能为空
     * @return 信息字符串
     */
    public static String getMessage(QiniuException e) {
        // 取 error 信息优先从 exception 的 message 中取，避免直接 e.error() 抛出非预期异常，因为 error() 方法底层可能会调用
        // response.isJson()，该方法底层会抛出空指针异常（位置 com.qiniu.http.Response.ctype(Response.java:137)），同时
        // getMessage 会包含 reqid 等信息
        String message = e.getMessage() == null ? "" : "message: " + e.getMessage();
        if ("".equals(message)) {
            message = (e.response != null ? "reqid: " + e.response.reqId + ", ": "") + "code: " + e.code();
            // 避免抛出空指针异常
            try { message += "， error: " + e.error(); } catch (Exception ex) { ex.printStackTrace(); }
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
        String message = e != null ? getMessage(e) : "";
        if (infoList == null || infoList.size() == 0) {
            fileMap.writeError(message.replaceAll("\n", "\t"), false);
        } else {
            fileMap.writeError(String.join("\n", infoList.stream()
                    .map(line -> line + "\t" + message.replaceAll("\n", "\t"))
                    .collect(Collectors.toList())), false);
        }
    }

    public static void writeLog(IOException e, FileMap fileMap, String info) throws IOException {
        fileMap.writeError(info + "\t" + e.getMessage().replaceAll("\n", "\t"), false);
    }

    public static void writeLog(IOException e, FileMap fileMap, List<String> infoList) throws IOException {
        if (infoList == null || infoList.size() == 0) {
            fileMap.writeError(e.getMessage().replaceAll("\n", "\t"), false);
        } else {
            fileMap.writeError(String.join("\n", infoList.stream()
                    .map(line -> line + "\t" + e.getMessage().replaceAll("\n", "\t"))
                    .collect(Collectors.toList())), false);
        }
    }
}
