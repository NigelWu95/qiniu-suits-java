package com.qiniu.util;

import com.qiniu.common.QiniuException;

public final class LogUtils {

    /**
     * 取出异常中的信息
     * @param e 异常对象，不能为空
     * @return 信息字符串
     */
    public static String getMessage(QiniuException e) {
        // 取 error 信息优先从 exception 的 message 中取，避免直接 e.error() 抛出非预期异常，因为 error() 方法底层可能会调用
        // response.isJson()，该方法底层会抛出空指针异常（位置 com.qiniu.http.Response.ctype(Response.java:137)），同时
        // getMessage 会包含 reqid 等信息
        String message = e.getMessage();
        if (message == null || "".equals(message)) {
            message = (e.response != null ? "reqid: " + e.response.reqId + ", ": "") + "code: " + e.code();
            // 避免抛出空指针异常
            try {
                message += "， error: " + e.error();
            } catch (Exception ex) {
                message += ", failed: " + ex.getMessage();
                ex.printStackTrace();
            }
        }
        return message.replace("\n", "  ");
    }
}
