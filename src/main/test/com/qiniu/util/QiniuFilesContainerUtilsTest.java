package com.qiniu.util;

import com.qiniu.storage.model.FileInfo;
import org.junit.Test;

import static org.junit.Assert.*;

public class QiniuFilesContainerUtilsTest {

    @Test
    public void testCalcMarker() {
        FileInfo fileInfo = new FileInfo();
        fileInfo.key = "test.gif";
        fileInfo.type = 0;
        System.out.println(ListBucketUtils.calcMarker(fileInfo));
    }

    @Test
    public void testDecodeMarker() {
        FileInfo fileInfo = ListBucketUtils.decodeMarker("eyJjIjowLCJrIjoidGVzdC5naWYifQ==");
        System.out.println(fileInfo.key + "\t" + fileInfo.type);
    }
}