package com.qiniu.service.ai;

import com.qiniu.model.CensorResp;
import com.qiniu.model.parameter.ListBucketParams;
import com.qiniu.util.Auth;
import org.junit.Test;

public class AudioStreamCensorTest {

    @Test
    public void testDoCensor() throws Exception {

        ListBucketParams listBucketParams = new ListBucketParams("resources/.qiniu.properties");
        String accessKey = listBucketParams.getAccessKey();
        String secretKey = listBucketParams.getSecretKey();
        VideoCensor videoCensor = VideoCensor.getInstance(Auth.create(accessKey, secretKey));
        videoCensor.setPulpOps("", "1", 2, 2, 1, "", "");
        videoCensor.setCensorParams(false, 1, 8, "", "", "");
        CensorResp censorResp = videoCensor.doCensor("12345678", "http://zb.xksquare.com/20180929204815_1_5baf748f1e18a.mp4");
        System.out.println(censorResp.getPulpResult());
    }
}