package com.qiniu.datasource;

import com.obs.services.ObsConfiguration;
import com.qiniu.config.PropertiesFile;
import com.qiniu.util.CloudAPIUtils;
import org.junit.Test;

import java.util.HashMap;

public class HuaweiObsContainerTest {

    @Test
    public void testContainer() throws Exception {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKeyId = propertiesFile.getValue("hua-id");
        String secretKey = propertiesFile.getValue("hua-secret");
        String bucket = propertiesFile.getValue("bucket");
        bucket = "css-backup-1544044401924";
        HuaweiObsContainer baiduObsContainer = new HuaweiObsContainer(accessKeyId, secretKey, new ObsConfiguration(),
                "https://obs." + CloudAPIUtils.getHuaweiObsRegion(accessKeyId, secretKey, bucket) + ".myhuaweicloud.com",
                bucket, null, null, false, false, new HashMap<String, String>(){{ put("key", "key"); }},
                null, 1000, 10);
        baiduObsContainer.setSaveOptions(true, "../huawei", "tab", "\t", null);
        baiduObsContainer.export();
    }
}