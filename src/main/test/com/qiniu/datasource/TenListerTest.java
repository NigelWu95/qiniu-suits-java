package com.qiniu.datasource;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.region.Region;
import com.qiniu.config.PropertiesFile;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TenListerTest {

    private TenLister tenLister;

    @Before
    public void init() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.tencent.properties");
        String secretId = propertiesFile.getValue("SecretId");
        String secretKey = propertiesFile.getValue("SecretKey");
        String regionName = propertiesFile.getValue("region");
        COSCredentials cred = new BasicCOSCredentials(secretId, secretKey);
        ClientConfig clientConfig = new ClientConfig(new Region(regionName));
        COSClient cosClient = new COSClient(cred, clientConfig);
        String bucket = propertiesFile.getValue("bucket");
        tenLister = new TenLister(cosClient, bucket, null, null, null, null, 10000);
    }

    @Test
    public void testGetStatusCode() {
        Assert.assertEquals(tenLister.getStatusCode(), 200);
    }

    @Test
    public void testGetError() {
        Assert.assertNull(tenLister.getError());
    }

    @Test
    public void testGetCosObjectList() {
    }

    @Test
    public void testGetMarker() {
        System.out.println(tenLister.getMarker());
    }

    @Test
    public void testCheckMarkerValid() {
    }

    @Test
    public void testCheckListValid() {
    }

    @Test
    public void testHasNext() {
        Assert.assertTrue(tenLister.hasNext());
    }

    @Test
    public void testNext() {
        while (tenLister.hasNext()) {
            tenLister.next();
        }
    }
}