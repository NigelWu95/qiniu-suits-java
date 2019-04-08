package com.qiniu.util;

import com.qiniu.storage.model.FileInfo;
import org.junit.Test;

public class OSSListUtilsTest {

    @Test
    public void testCalcMarker() {
        FileInfo fileInfo = new FileInfo();
        fileInfo.key = "test.gif";
        fileInfo.type = 0;
        System.out.println(OSSListUtils.calcMarker(fileInfo));
    }

    @Test
    public void testDecodeMarker() {
        FileInfo fileInfo = OSSListUtils.decodeMarker("eyJjIjowLCJrIjoidGVzdC5naWYifQ==");
        System.out.println(fileInfo.key + "\t" + fileInfo.type);
    }
}