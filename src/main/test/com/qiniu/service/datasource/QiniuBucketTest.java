package com.qiniu.service.datasource;

import com.qiniu.common.Zone;
import com.qiniu.model.ListBucketParams;
import com.qiniu.sdk.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class QiniuBucketTest {

    private QiniuBucket qiniuBucket;

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
        this.qiniuBucket = new QiniuBucket(auth, configuration, bucket, unitLen, version, customPrefix,
                antiPrefix, 1);
    }

    @Test
    public void testConcurrentlyList() {
        qiniuBucket.concurrentlyList(10, 2, null);
    }

    @Test
    public void testCheckValidPrefix() {
        qiniuBucket.checkValidPrefix(1);
    }

    @Test
    public void testStraightlyList() {
        qiniuBucket.straightlyList(null, null, null);
    }
}