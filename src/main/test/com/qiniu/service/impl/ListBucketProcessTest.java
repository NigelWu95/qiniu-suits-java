package com.qiniu.service.impl;

import com.qiniu.common.ListFileAntiFilter;
import com.qiniu.common.ListFileFilter;
import com.qiniu.common.QiniuException;
import com.qiniu.common.Zone;
import com.qiniu.http.Response;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.model.ListBucketParams;
import com.qiniu.model.ListResult;
import com.qiniu.service.oss.ListBucket;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class ListBucketProcessTest {

    private ListBucketProcess listBucketProcess;
    private ListBucket listBucket;

    private Auth auth;
    private Configuration configuration;
    private String bucket;
    private int unitLen;
    private int version;

    @Before
    public void get() throws Exception {
        ListBucketParams listBucketParams = new ListBucketParams("resources/.qiniu.properties");
        String accessKey = listBucketParams.getAccessKey();
        String secretKey = listBucketParams.getSecretKey();
        String resultFileDir = listBucketParams.getResultFileDir();
        String customPrefix = listBucketParams.getCustomPrefix();
        List<String> antiPrefix = listBucketParams.getAntiPrefix();
        this.bucket = listBucketParams.getBucket();
        this.version = listBucketParams.getVersion();
        this.unitLen = listBucketParams.getUnitLen();
        this.unitLen = (version == 1 && unitLen > 1000) ? unitLen%1000 : unitLen;
        this.auth = Auth.create(accessKey, secretKey);
        this.configuration = new Configuration(Zone.autoZone());
        this.listBucket = new ListBucket(auth, configuration);
        this.listBucketProcess = new ListBucketProcess(auth, configuration, bucket, unitLen, version, resultFileDir,
                customPrefix, antiPrefix, 1);
    }

    @Test
    public void testGetListResult() throws Exception {
        Response response = listBucket.run(bucket, "e", "", "", unitLen, 1, version);
        ListResult listResult = listBucketProcess.getListResult(response, version);
        response.close();
        Assert.assertTrue(listResult.isValid());
        Assert.assertEquals(1, listResult.fileInfoList.size());
    }

    @Test
    public void testStraightList() throws IOException {
        listBucketProcess.straightList("", "", "", null, false);
    }
}