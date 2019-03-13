package com.qiniu.util;

import java.util.concurrent.atomic.AtomicBoolean;

public class SystemUtils {

    /**
     * 程序退出方法，用于在多线程情况下某个线程出现异常时退出程序。当线程池中的程序遇到异常时会执行该方法退出程序，如果同时多个线程抛出异常则通过
     * exitBool 来判断是否已经执行过退出程序，故只输出一次异常信息
     * @param exitBool 多线程的原子操作 bool 值，初始值应该为 false
     * @param e 异常对象
     */
    synchronized static public void exit(AtomicBoolean exitBool, Exception e) {
        if (!exitBool.get()) e.printStackTrace();
        exitBool.set(true);
        System.exit(-1);
    }
}
