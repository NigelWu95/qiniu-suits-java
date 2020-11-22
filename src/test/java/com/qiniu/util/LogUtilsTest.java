package com.qiniu.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class LogUtilsTest {

    @Test
    public void testGetLogPath() {
        System.out.println(LogUtils.getLogPath(LogUtils.QSUITS));
        System.out.println(System.getProperty("qsuits"));
        System.out.println(System.getProperty("procedure"));
        System.out.println(LogUtils.getLogPath(LogUtils.PROCEDURE));
        System.out.println(System.getProperty("qsuits"));
        System.out.println(System.getProperty("procedure"));
    }
}