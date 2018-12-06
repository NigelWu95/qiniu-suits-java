package com.qiniu.service.datasource;

import com.qiniu.common.QiniuException;
import com.qiniu.common.Zone;
import com.qiniu.model.parameter.ListBucketParams;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ListBucketTest {

    private ListBucket listBucket;

    @Before
    public void init() throws Exception {
        ListBucketParams listBucketParams = new ListBucketParams("resources/.qiniu.properties");
        String accessKey = listBucketParams.getAccessKey();
        String secretKey = listBucketParams.getSecretKey();
        Auth auth = Auth.create(accessKey, secretKey);
        Configuration configuration = new Configuration(Zone.autoZone());
        String customPrefix = listBucketParams.getCustomPrefix();
        List<String> antiPrefix = listBucketParams.getAntiPrefix();
        String bucket = listBucketParams.getBucket();
        int version = listBucketParams.getVersion();
        int unitLen = listBucketParams.getUnitLen();
        unitLen = (version == 1 && unitLen > 1000) ? unitLen %1000 : unitLen;
        this.listBucket = new ListBucket(auth, configuration, bucket, unitLen, version, customPrefix,
                antiPrefix, 1);
    }

    @Test
    public void testConcurrentlyList() throws QiniuException {
        listBucket.concurrentlyList(15, 1, null);
    }

    @Test
    public void testStraightlyList() throws IOException {
        listBucket.straightlyList(null, null, null);
    }
}