package com.qiniu.util;

import com.qiniu.common.QiniuException;
import com.qiniu.common.SuitsException;
import com.qiniu.http.Response;

import java.io.IOException;

public final class HttpRespUtils {

    /**
     * 判断 process 产生（不适用于 datasource 读取产生的异常）的异常结果，返回后续处理标志
     * @param e 需要处理的 QiniuException 异常
     * @param times 此次处理失败前的重试次数，如果已经为小于 1 的话则说明没有重试机会
     * @return 返回重试次数，返回 -2 表示该异常应该抛出，返回 -1 表示重试次数已用尽，可以记录为待重试信息，返回 0 表示该异常应该记录并跳过，返
     * 回大于 0 表示可以进行重试
     */
    public static int checkException(QiniuException e, int times) {
        // 处理一次异常返回后的重试次数应该减少一次，并且可用于后续判断是否有重试的必要
        times--;
        if (e.response != null) {
            int code = e.code();
            if (times <= 0 && code == 599) {
                return -2; // 如果重试次数为 0 且响应的是 599 状态码则抛出异常
            } else if (code < 0 || code == 406 || code == 429 || (code >= 500 && code < 600 && code != 579)) {
                return times; // 429 和 573 为请求过多的状态码，可以进行重试
            } else if ((e.code() >= 400 && e.code() <= 499) || (e.code() >= 612 && e.code() <= 614) || e.code() == 579) {
                return 0; // 避免因为某些可暂时忽略和记录的状态码导致程序中断故先处理该异常返回 0
            } else { // 如 631 状态码表示空间不存在，则不需要重试抛出异常
                return -2;
            }
        } else {
            if (e.error() != null || e.getMessage() != null) {
                return 0;
            } else if (times <= 0) {
                return -1;
            } else {
                return times; // 请求超时等情况下可能异常中的 response 为空，需要重试
            }
        }
    }

    // 检查七牛 API 请求返回的状态码，返回 1 表示成功，返回 0 表示需要重试，返回 -1 表示可以记录错误
    public static int checkStatusCode(int code) {
        if (code == 200) {
            return 1;
        } else if (code <= 0 || code == 406 || code == 429 || (code >= 500 && code < 600)) {
            return 0;
        } else {
            return -1;
        }
    }

    public static int listExceptionWithRetry(SuitsException e, int retry) throws SuitsException {
        // date offset error 在部分数据源（如 upyun）中出现，可能是由于签名时间误差导致，可重试
        if (e.getStatusCode() == 401 && e.getMessage().contains("date offset error")) {
            retry--;
        } else if (e.getStatusCode() == 429 || e.getStatusCode() == 509 || e.getStatusCode() == 571 || e.getStatusCode() == 573) {
            retry--;
            try {
                Thread.sleep(3000);
            } catch (InterruptedException interruptEx) {
                e.addSuppressed(interruptEx);
            }
        } else if (e.getStatusCode() >= 500 && e.getStatusCode() < 600) {
            if (retry < 0) throw e;
            else {
                retry--;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException interruptEx) {
                    e.addSuppressed(interruptEx);
                    throw e;
                }
                return retry;
            }
        } else if ((e.getStatusCode() >= 400 && e.getStatusCode() != 406) || e.getStatusCode() >= 600) {
            throw e;
        } else {
            retry--;
        }
        return retry;
    }

    /**
     * 将 Response 对象转换成为结果字符串
     * @param response 得到的 Response 对象
     * @return Response body 转换的 String 对象
     * @throws IOException Response 非正常响应的情况下抛出的异常
     */
    public static String getResult(Response response) throws IOException {
        if (response == null) throw new IOException("empty response");
        if (response.statusCode != 200 && response.statusCode != 298) throw new QiniuException(response);
        String responseBody = response.bodyString();
        response.close();
        return responseBody;
    }
}
