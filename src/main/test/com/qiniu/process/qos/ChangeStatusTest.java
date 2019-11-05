package com.qiniu.process.qos;

import com.qiniu.config.PropertiesFile;
import com.qiniu.storage.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ChangeStatusTest {

    private ChangeStatus changeStatus;

    @Before
    public void init() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        String bucket = propertiesFile.getValue("bucket");
        changeStatus = new ChangeStatus(accessKey, secretKey, new Configuration(), bucket, 1, "../temp");
    }

    @Test
    public void testSingleResult() {
        Map<String, String> map = new HashMap<String, String>(){{
            put("key", "qiniu_success_1.txt");
        }};
        try {
            String result = changeStatus.processLine(map);
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}