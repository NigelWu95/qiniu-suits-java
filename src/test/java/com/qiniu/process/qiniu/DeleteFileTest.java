package com.qiniu.process.qiniu;

import com.qiniu.config.PropertiesFile;
import com.qiniu.storage.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DeleteFileTest {

    @Test
    public void testSingle() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        String bucket = propertiesFile.getValue("bucket");
        DeleteFile deleteFile = new DeleteFile(accessKey, secretKey, new Configuration(), "ts-work", "../temp");
        Map<String, String> map = new HashMap<String, String>(){{
            put("key", "qiniu_success_1-1.txt");
        }};
        try {
            String result = deleteFile.processLine(map);
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}