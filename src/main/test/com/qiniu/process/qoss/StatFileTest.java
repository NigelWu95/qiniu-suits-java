package com.qiniu.process.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.config.PropertiesFile;
import com.qiniu.storage.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class StatFileTest {

    private StatFile statFile;

    @Before
    public void init() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        String bucket =
                "note-video";
//                propertiesFile.getValue("bucket");
        statFile = new StatFile(accessKey, secretKey, new Configuration(), bucket, "../xhs", "tab", "\t");
    }
    @Test
    public void testSingleResult() {
        Map<String, String> map = new HashMap<String, String>(){{
            put("key", "4969afce9276446daa97be8a199cffd2.html");
        }};
        try {
            String result = statFile.singleResult(map);
            System.out.println(result);
        } catch (QiniuException e) {
            e.printStackTrace();
        }
    }
}