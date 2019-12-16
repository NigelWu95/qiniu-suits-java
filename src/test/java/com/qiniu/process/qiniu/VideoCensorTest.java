package com.qiniu.process.qiniu;

import com.qiniu.config.PropertiesFile;
import com.qiniu.process.qiniu.VideoCensor;
import com.qiniu.storage.Configuration;
import org.junit.Test;

import java.util.HashMap;

public class VideoCensorTest {

    @Test
    public void singleResult() throws Exception {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        VideoCensor videoCensor = new VideoCensor(accessKey, secretKey, new Configuration(), null, null,
                "url", new String[]{"pulp"}, 0, null, null, null);
        String result = videoCensor.singleResult(new HashMap<String, String>(){{
            put("url", "http://p3l1d5mx4.bkt.clouddn.com/-YVzTgC_I8zlDYIm8eCcPnA76pU=/ltSP7XPbPGviBNjXiZEHX7mpdm6o");
        }});
        System.out.println(result);
    }
}