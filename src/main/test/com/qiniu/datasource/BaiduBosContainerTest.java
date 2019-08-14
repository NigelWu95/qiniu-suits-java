package com.qiniu.datasource;

import com.baidubce.services.bos.BosClientConfiguration;
import com.qiniu.config.PropertiesFile;
import org.junit.Test;

import java.util.HashMap;

public class BaiduBosContainerTest {

    @Test
    public void testContainer() throws Exception {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKeyId = propertiesFile.getValue("bai-id");
        String secretKey = propertiesFile.getValue("bai-secret");
        String bucket = propertiesFile.getValue("bucket");
        bucket = "nigel-test";
        BaiduBosContainer baiduObsContainer = new BaiduBosContainer(accessKeyId, secretKey, new BosClientConfiguration(),
                "su.bcebos.com", bucket, null, null,
                false, false, new HashMap<String, String>(){{ put("key", "key"); }}, null,
                1000, 10);
        baiduObsContainer.setSaveOptions(true, "../baidu", "tab", "\t", null);
        baiduObsContainer.export();
    }

}