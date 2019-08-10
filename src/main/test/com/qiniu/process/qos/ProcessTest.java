package com.qiniu.process.qos;

import com.qiniu.config.PropertiesFile;
import com.qiniu.process.qdora.QiniuPfop;
import com.qiniu.storage.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ProcessTest {

    private StatFile statFile;
    private QiniuPfop qiniuPfop;

    @Before
    public void init() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        String bucket =
//                "note-video";
                propertiesFile.getValue("bucket");
        statFile = new StatFile(accessKey, secretKey, new Configuration(), bucket, "tab", "\t", null);
        qiniuPfop = new QiniuPfop(accessKey, secretKey, new Configuration(), bucket, null, null, null, "1");
    }
    @Test
    public void testResult() {
        Map<String, String> map = new HashMap<String, String>(){{
//            put("key", "welcome.html");
        }};
        try {
//            String result = statFile.processLine(map);
            String result = qiniuPfop.processLine(map);
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}