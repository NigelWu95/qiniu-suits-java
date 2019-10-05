package com.qiniu.process.qos;

import com.qiniu.config.PropertiesFile;
import com.qiniu.process.other.DownloadFile;
import com.qiniu.storage.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.*;

public class UploadFileTest {

    @Test
    public void testSingleResult() {
        try {
            PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
            String accessKey = propertiesFile.getValue("ak");
            String secretKey = propertiesFile.getValue("sk");
            String bucket = propertiesFile.getValue("bucket");
            UploadFile uploadFile = new UploadFile(accessKey, secretKey, new Configuration(), bucket, null, null,
                    true, true, null, null, 3600, null, null, true);
            String result = uploadFile.processLine(new HashMap<String, String>(){{
                put("path", "/Users/wubingheng/Downloads/append_test.go");
//                put("filepath", "/Users/wubingheng/Downloads/append_test.go");
//                put("key", "/Users/wubingheng/Downloads/append_test.go");
            }});
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}