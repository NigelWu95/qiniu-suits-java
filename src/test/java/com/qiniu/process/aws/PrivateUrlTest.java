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
        String bucket = "7zmz4b";
        String region = "cn-east-1";
        String endpoint = "http://s3-cn-east-1.qiniucs.com";
        PrivateUrl privateUrl = new PrivateUrl(accessKeyId, accessKeySecret, bucket, endpoint, region, 3600000, null, "~/Downloads");
        String result = privateUrl.singleResult(new HashMap<String, String>(){{
            put("key", "0000021");
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