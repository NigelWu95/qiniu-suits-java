package com.qiniu.process.huawei;

import com.qiniu.config.PropertiesFile;
import com.qiniu.util.CloudApiUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrivateUrlTest {

    @Test
    public void testProcessLine() throws Exception {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKeyId = propertiesFile.getValue("hua-id");
        String accessKeySecret = propertiesFile.getValue("hua-secret");
        String bucket = "css-backup-1544044401924";
        String endPoint = "https://obs." + CloudApiUtils.getHuaweiObsRegion(accessKeyId, accessKeySecret, bucket) + ".myhuaweicloud.com";
        PrivateUrl privateUrl = new PrivateUrl(accessKeyId, accessKeySecret, bucket, endPoint, 3600, null, "~/Downloads");
        String result = privateUrl.singleResult(new HashMap<String, String>(){{
            put("key", "24790f63-0936-44c4-8695-a6d6b1dd8d91.jpg");
        }});
        System.out.println(result);

        List<Map<String, String>> mapList = new ArrayList<>();
        mapList.add(new HashMap<String, String>(){{ put("key", "1"); }});
        mapList.add(new HashMap<String, String>(){{ put("key", "2"); }});
        mapList.add(new HashMap<String, String>(){{ put("key", "3"); }});
        privateUrl.processLine(mapList);
        privateUrl.closeResource();
    }
}