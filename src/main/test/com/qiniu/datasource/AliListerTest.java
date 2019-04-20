package com.qiniu.datasource;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.common.auth.*;
import com.aliyun.oss.model.OSSObjectSummary;
import com.qiniu.common.SuitsException;
import com.qiniu.config.PropertiesFile;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class AliListerTest {

    private AliLister aliLister;

    @Before
    public void init() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.ali.properties");
        String endpoint = propertiesFile.getValue("endpoint");
        String accessKeyId = propertiesFile.getValue("accessKeyId");
        String accessKeySecret = propertiesFile.getValue("accessKeySecret");
        String bucket = propertiesFile.getValue("bucket");
        CredentialsProvider credentialsProvider = new DefaultCredentialProvider(accessKeyId, accessKeySecret);
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        OSSClient ossClient = new OSSClient(endpoint, credentialsProvider, clientConfiguration);
        aliLister = new AliLister(ossClient, bucket, "p", null, null, null, 10);
    }

    @Test
    public void listForward() throws SuitsException {
        while (aliLister.hasNext()) {
            aliLister.listForward();
        }
    }

    @Test
    public void hasNext() {
        Assert.assertTrue(aliLister.hasNext());
    }

    @Test
    public void currents() {
        List<OSSObjectSummary> summaries = aliLister.currents();
        System.out.println(summaries.size());
    }

    @Test
    public void updateMarkerBy() {
    }

    @Test
    public void close() {
    }
}