package com.qiniu.sdk;

import com.qiniu.common.QiniuException;
import com.qiniu.common.Zone;
import com.qiniu.model.parameter.ListBucketParams;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.Auth;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class BucketManagerTest {

    private BucketManager bucketManager;
    private String bucket;

    @Before
    public void init() throws Exception {
        ListBucketParams listBucketParams = new ListBucketParams("resources/.qiniu.properties");
        String accessKey = listBucketParams.getAccessKey();
        String secretKey = listBucketParams.getSecretKey();
        Auth auth = Auth.create(accessKey, secretKey);
        Configuration configuration = new Configuration(Zone.autoZone());
        this.bucket = listBucketParams.getBucket();
        this.bucketManager = new BucketManager(auth, configuration);
    }

    @Test
    public void testListFilesV2() {
        try {
            FileListing fileListing = bucketManager.listFilesV2(bucket, "", "", 1000, "/");
            System.out.println(Arrays.toString(fileListing.items));
            System.out.println(Arrays.toString(fileListing.commonPrefixes));
            System.out.println(fileListing.marker);
        } catch (QiniuException e) {
            e.printStackTrace();
            e.response.close();
        }
    }

}