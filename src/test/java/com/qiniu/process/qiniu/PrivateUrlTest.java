package com.qiniu.process.qiniu;

import com.qiniu.config.PropertiesFile;
import org.junit.Test;

import java.util.HashMap;

public class PrivateUrlTest {

    @Test
    public void singleResult() throws Exception {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        PrivateUrl privateUrl = new PrivateUrl(accessKey, secretKey, null, null, "url", "-abc", 3600);
        String result = privateUrl.singleResult(new HashMap<String, String>(){{
            put("url", "http://xxx.cn/upload/24790f63-0936-44c4-8695-a6d6b1dd8d91.jpg");
        }});
        System.out.println(result);
    }
}