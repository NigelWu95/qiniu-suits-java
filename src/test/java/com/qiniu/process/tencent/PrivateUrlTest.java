package com.qiniu.process.tencent;

import com.qiniu.config.PropertiesFile;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class PrivateUrlTest {

    @Test
    public void testProcessLine() throws Exception {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKeyId = propertiesFile.getValue("ten-id");
        String accessKeySecret = propertiesFile.getValue("ten-secret");
        String bucket = propertiesFile.getValue("bucket");
        String region = "ap-shanghai";
        PrivateUrl privateUrl = new PrivateUrl(accessKeyId, accessKeySecret, bucket, region, false, 3600, null, "~/Downloads");
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