package com.qiniu.process.aliyun;

import com.qiniu.config.PropertiesFile;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

public class PrivateUrlTest {

    @Test
    public void singleResult() throws Exception {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKeyId = propertiesFile.getValue("ali-id");
        String accessKeySecret = propertiesFile.getValue("ali-secret");
        String bucket = propertiesFile.getValue("bucket");
        String endpoint = propertiesFile.getValue("region");
        PrivateUrl privateUrl = new PrivateUrl(accessKeyId, accessKeySecret, bucket, endpoint, 3600, null);
        String result = privateUrl.singleResult(new HashMap<String, String>(){{
            put("key", "24790f63-0936-44c4-8695-a6d6b1dd8d91.jpg");
        }});
        System.out.println(result);
    }

    @Test
    public void testProcessLine() throws Exception {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKeyId = propertiesFile.getValue("ali-id");
        String accessKeySecret = propertiesFile.getValue("ali-secret");
        String bucket = propertiesFile.getValue("bucket");
        String endpoint = propertiesFile.getValue("region");
        PrivateUrl privateUrl = new PrivateUrl(accessKeyId, accessKeySecret, bucket, endpoint, 3600, null, "~/Downloads");
        List<Map<String, String>> mapList = new ArrayList<>();
        mapList.add(new HashMap<String, String>(){{ put("key", "1"); }});
        mapList.add(new HashMap<String, String>(){{ put("key", "2"); }});
        mapList.add(new HashMap<String, String>(){{ put("key", "3"); }});
        List<Map<String, String>> mapList2 = new ArrayList<>();
        mapList2.add(new HashMap<String, String>(){{ put("key", "5"); }});
        mapList2.add(new HashMap<String, String>(){{ put("key", "6"); }});
        mapList2.add(new HashMap<String, String>(){{ put("key", "7"); }});
        PrivateUrl clonedPrivateUrl = privateUrl.clone();
        ExecutorService pool = Executors.newCachedThreadPool();
        pool.execute(() -> {
            try {
                clonedPrivateUrl.processLine(mapList2);
                clonedPrivateUrl.closeResource();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        pool.execute(() -> {
            try {
                privateUrl.processLine(mapList);
                privateUrl.closeResource();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        pool.shutdown();
    }
}