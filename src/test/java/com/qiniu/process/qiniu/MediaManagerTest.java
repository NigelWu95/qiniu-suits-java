package com.qiniu.process.qiniu;

import com.qiniu.common.QiniuException;
import com.qiniu.process.qiniu.MediaManager;
import org.junit.Test;

public class MediaManagerTest {

    @Test
    public void testGetAvinfo() throws QiniuException {
        MediaManager mediaManager = new MediaManager();
        System.out.println(mediaManager.getAvinfoBody("http://p3l1d5mx4.bkt.clouddn.com/123456aaa.jpg "));
    }
}