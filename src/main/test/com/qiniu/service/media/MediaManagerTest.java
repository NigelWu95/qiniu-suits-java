package com.qiniu.service.media;

import com.qiniu.common.QiniuException;
import com.qiniu.model.media.Avinfo;
import com.qiniu.util.JsonConvertUtils;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;

import static org.junit.Assert.*;

public class MediaManagerTest {

    private MediaManager mediaManager;

    @Before
    public void init() {
        this.mediaManager = new MediaManager();
    }

    @Test
    public void testGetAvinfo() throws QiniuException, UnknownHostException {
        Avinfo avinfo = mediaManager.getAvinfo("http://temp.nigel.qiniuts.com/1531712104620.mp4");
        System.out.println(JsonConvertUtils.toJsonWithoutUrlEscape(avinfo));
    }

    @Test
    public void testGetAvinfo1() {
    }
}