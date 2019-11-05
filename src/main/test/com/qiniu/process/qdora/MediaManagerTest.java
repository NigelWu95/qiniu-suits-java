package com.qiniu.process.qdora;

import com.qiniu.common.QiniuException;
import org.junit.Test;

import static org.junit.Assert.*;

public class MediaManagerTest {

    @Test
    public void testGetAvinfo() throws QiniuException {
        MediaManager mediaManager = new MediaManager();
        System.out.println(mediaManager.getAvinfoBody("http://p3l1d5mx4.bkt.clouddn.com/123456aaa.jpg "));
    }
}