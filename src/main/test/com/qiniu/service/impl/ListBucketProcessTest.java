package com.qiniu.service.impl;

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
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class ListBucketProcessTest {

    @Test
    public void testGetListResult() throws Exception {
        ListBucketParams listBucketParams = new ListBucketParams("resources/.qiniu.properties");
        String accessKey = listBucketParams.getAccessKey();
        String secretKey = listBucketParams.getSecretKey();
        String bucket = listBucketParams.getBucket();
        String resultFileDir = listBucketParams.getResultFileDir();
        int version = listBucketParams.getVersion();
        int unitLen = listBucketParams.getUnitLen();
        unitLen = (version == 1 && unitLen > 1000) ? unitLen%1000 : unitLen;
        String customPrefix = listBucketParams.getCustomPrefix();
        List<String> antiPrefix = listBucketParams.getAntiPrefix();
        Auth auth = Auth.create(accessKey, secretKey);
        Configuration configuration = new Configuration(Zone.autoZone());
        ListBucket listBucket = new ListBucket(auth, configuration);
        Response response = listBucket.run(bucket, "e", "", "", unitLen, 1, version);
        ListBucketProcess listBucketProcess = new ListBucketProcess(auth, configuration, bucket, unitLen, version, resultFileDir,
                customPrefix, antiPrefix, 1);
        ListResult listResult = listBucketProcess.getListResult(response, version);
        response.close();
        Assert.assertTrue(listResult.isValid());
        Assert.assertEquals(1, listResult.fileInfoList.size());
    }
}