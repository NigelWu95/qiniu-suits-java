package com.qiniu.process.aws;

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
        String accessKeyId = propertiesFile.getValue("s3-id");
        String accessKeySecret = propertiesFile.getValue("s3-secret");
        String bucket = "nigel1";
        String region = "ap-southeast-1";
        PrivateUrl privateUrl = new PrivateUrl(accessKeyId, accessKeySecret, bucket, region, 3600, null, "~/Downloads");
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