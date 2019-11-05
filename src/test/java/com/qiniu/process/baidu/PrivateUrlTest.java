package com.qiniu.process.baidu;

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
        String accessKeyId = propertiesFile.getValue("bai-id");
        String accessKeySecret = propertiesFile.getValue("bai-secret");
        String bucket = "nigel-test";
        String endPoint = "https://" + CloudApiUtils.getBaiduBosRegion(accessKeyId, accessKeySecret, bucket) + ".bcebos.com";
        PrivateUrl privateUrl = new PrivateUrl(accessKeyId, accessKeySecret, bucket, endPoint, 3600, null, "~/Downloads");
        String result = privateUrl.singleResult(new HashMap<String, String>(){{
            put("key", "UpYosContainer.class");
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