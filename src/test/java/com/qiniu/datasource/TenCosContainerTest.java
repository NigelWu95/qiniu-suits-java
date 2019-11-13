package com.qiniu.datasource;

import com.google.gson.JsonObject;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.region.Region;
import com.qiniu.config.PropertiesFile;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class TenCosContainerTest {

    private TenCosContainer tenCosContainer;

    @Before
    public void init() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.tencent.properties");
        String secretId = propertiesFile.getValue("ten-id");
        String secretKey = propertiesFile.getValue("ten-secret");
        String regionName = propertiesFile.getValue("region");
        COSCredentials cred = new BasicCOSCredentials(secretId, secretKey);
        ClientConfig clientConfig = new ClientConfig(new Region(regionName));
        String bucket
//                = propertiesFile.getValue("bucket");
                = "test-1254031816";
        tenCosContainer = new TenCosContainer(secretId, secretKey, clientConfig, bucket, null, null,
                false, false, new HashMap<String, String>(){{ put("key", "key"); }}, null,
                1000, 10);
        tenCosContainer.setSaveOptions(true, "../tencent", "tab", "\t", null);
    }

    @Test
    public void export() throws Exception {
        tenCosContainer.export();
    }

    @Test
    public void testPrefixConfig() {
        tenCosContainer.recordListerByPrefix("a");
        JsonObject json = new JsonObject();
        json.addProperty("start", "a");
    }

    @Test
    public void testWriteContinuedPrefixConfig() throws IOException {
        File file = new File("./");
        System.out.println(file);
        System.out.println(file.exists());
        System.out.println(file.isDirectory());
        System.out.println(file.getCanonicalPath());
        System.out.println(file.getAbsolutePath());
        file = new File(file.getAbsolutePath());
        System.out.println(file.getParent());
        tenCosContainer.endAction();
    }
}