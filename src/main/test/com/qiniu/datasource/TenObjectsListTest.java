package com.qiniu.datasource;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.region.Region;
import com.qiniu.config.PropertiesFile;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.*;

public class TenObjectsListTest {

    private TenObjectsList tenObjectsList;

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
        tenObjectsList = new TenObjectsList(secretId, secretKey, clientConfig, bucket, null, null,
                false, false, null, 1000, 10, "../tencent");
        tenObjectsList.setSaveOptions(true, "tab", "\t", null);
    }

    @Test
    public void export() throws Exception {
        tenObjectsList.export();
    }
}