package com.qiniu.util;

import java.util.concurrent.ExecutorService;

public class ExecutorsUtils {

    public static void waitForShutdown(ExecutorService executorPool, String info) {
        try {
            while (!executorPool.isTerminated()) Thread.sleep(1000);
            System.out.println(info + " finished");
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}