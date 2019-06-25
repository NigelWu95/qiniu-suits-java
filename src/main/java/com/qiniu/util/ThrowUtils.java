package com.qiniu.util;

import com.qiniu.common.SuitsException;

import java.util.concurrent.atomic.AtomicBoolean;

public final class ThrowUtils {

    /**
     * 程序退出方法，用于在多线程情况下某个线程出现异常时退出程序，如果同时多个线程抛出异常则通过 exitBool 来判断是否已经执行过退出程序，故只输出
     * 一次异常信息
     * @param exitBool 多线程的原子操作 bool 值，初始值应该为 false
     * @param e 异常对象
     */
    synchronized static public void exit(AtomicBoolean exitBool, Throwable e) {
        if (!exitBool.get()) e.printStackTrace();
        exitBool.set(true);
        System.exit(-1);
    }

    public static int listExceptionWithRetry(SuitsException e, int retry) throws SuitsException {
        if (e.getStatusCode() == 401 && e.getMessage().contains("date offset error")) {
            retry--;
        } else if (e.getStatusCode() == 429) {
            try { Thread.sleep(3000); } catch (InterruptedException ignored) {}
        } else if (HttpRespUtils.checkStatusCode(e.getStatusCode()) < 0 || (retry <= 0 && e.getStatusCode() >= 500)) {
            throw e;
        } else {
            retry--;
        }
        return retry;
    }
}
