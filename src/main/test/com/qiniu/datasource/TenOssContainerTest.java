package com.qiniu.datasource;

import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.region.Region;
import com.qiniu.config.PropertiesFile;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TenOssContainerTest {

    private TenFilesContainer tenFilesContainer;

    @Before
    public void init() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.tencent.properties");
        String secretId = propertiesFile.getValue("SecretId");
        String secretKey = propertiesFile.getValue("SecretKey");
        String regionName = propertiesFile.getValue("region");
        COSCredentials cred = new BasicCOSCredentials(secretId, secretKey);
        ClientConfig clientConfig = new ClientConfig(new Region(regionName));
        String bucket = propertiesFile.getValue("bucket");
        tenFilesContainer = new TenFilesContainer(secretId, secretKey, clientConfig, bucket, null, null,
                false, false, null, 1000, 10, "../tencent");
        tenFilesContainer.setSaveOptions(true, "tab", "\t", null);
    }

    @Test
    public void export() throws Exception {
        tenFilesContainer.export();
    }
}