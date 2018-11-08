package com.qiniu.service.oss;

import com.qiniu.common.QiniuException;
import com.qiniu.common.Zone;
import com.qiniu.http.Response;
import com.qiniu.model.ListBucketParams;
import com.qiniu.service.impl.ListBucketProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class ListBucketTest {

    private ListBucket listBucket;
    private String bucket;

    @Before
    public void init() throws Exception {
        ListBucketParams listBucketParams = new ListBucketParams("resources/.qiniu.properties");
        String accessKey = listBucketParams.getAccessKey();
        String secretKey = listBucketParams.getSecretKey();
        Auth auth = Auth.create(accessKey, secretKey);
        Configuration configuration = new Configuration(Zone.autoZone());
        this.bucket = listBucketParams.getBucket();
        this.listBucket = new ListBucket(auth, configuration);
    }

    @Test
    public void run() {
        Response response;
        try {
            response = listBucket.run(bucket, "", "/", "", 1000, 3, 2);
            System.out.println(response.bodyString());
            response.close();
        } catch (QiniuException e) {
            e.printStackTrace();
            e.response.close();
        }
    }
}