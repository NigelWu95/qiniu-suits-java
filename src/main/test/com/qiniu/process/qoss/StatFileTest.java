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
        statFile = new StatFile(accessKey, secretKey, new Configuration(), bucket, "../xhs.txt", "tab", "\t");
    }
    @Test
    public void testSingleResult() {
        Map<String, String> map = new HashMap<String, String>(){{
            put("key", "bf86388c-3c84-d238-d6da-46924b6e4c17.html");
        }};
        try {
            String result = statFile.singleResult(map);
            System.out.println(result);
        } catch (QiniuException e) {
            e.printStackTrace();
        }
    }
}