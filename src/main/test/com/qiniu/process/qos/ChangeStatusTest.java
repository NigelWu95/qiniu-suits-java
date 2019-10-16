package com.qiniu.process.qos;

import com.qiniu.common.QiniuException;
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
        changeStatus = new ChangeStatus(accessKey, secretKey, new Configuration(), bucket, 1, "../xhs");
    }

    @Test
    public void testSingleResult() {
        Map<String, String> map = new HashMap<String, String>(){{
            put("key", "post/d6649026-952b-9f20-a542-29554f9f952d.html");
        }};
        try {
            String result = changeStatus.processLine(map);
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}