package com.qiniu.service.oss;

import com.qiniu.common.QiniuException;
import com.qiniu.common.Zone;
import com.qiniu.model.ListBucketParams;
import com.qiniu.sdk.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FileListerTest {

    private BucketManager bucketManager;
    private String bucket;
    private int unitLen;
    private int version;
    private FileLister fileLister;

    @Before
    public void init() throws Exception {
        ListBucketParams listBucketParams = new ListBucketParams("resources/.qiniu.properties");
        String accessKey = listBucketParams.getAccessKey();
        String secretKey = listBucketParams.getSecretKey();
        Auth auth = Auth.create(accessKey, secretKey);
        Configuration configuration = new Configuration(Zone.autoZone());
        this.bucket = listBucketParams.getBucket();
        this.version = listBucketParams.getVersion();
        this.unitLen = listBucketParams.getUnitLen();
        this.bucketManager = new BucketManager(auth, configuration);
    }

    @Test
    public void testHasNext() throws QiniuException {
        fileLister = new FileLister(bucketManager, bucket, "", "", "", unitLen,
                version, 3);
        Assert.assertTrue(fileLister.hasNext());
    }

    @Test
    public void testNext() throws QiniuException {
        fileLister = new FileLister(bucketManager, bucket, "", "", "", unitLen,
                version, 3);
        System.out.println(fileLister.next().size());
    }
}