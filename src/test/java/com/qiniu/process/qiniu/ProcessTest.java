package com.qiniu.process.qiniu;

import com.qiniu.config.PropertiesFile;
import com.qiniu.storage.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ProcessTest {

    private MoveFile moveFile;
    private StatFile statFile;
    private QiniuPfop qiniuPfop;

    @Before
    public void init() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        String bucket =
                "note-video";
//                propertiesFile.getValue("bucket");
        moveFile = new MoveFile(accessKey, secretKey, new Configuration(), bucket, bucket, "to-key", null, null, true);
        statFile = new StatFile(accessKey, secretKey, new Configuration(), bucket, "tab", "\t", null);
        qiniuPfop = new QiniuPfop(accessKey, secretKey, new Configuration(), bucket, null, null, false, null, null, "1");
    }

    @Test
    public void testPfopResult() {
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

    @Test
    public void testMoveResult() {
        Map<String, String> map = new HashMap<String, String>(){{
            put("key", "index.html");
            put("to-key", "");
        }};
        try {
//            String result = statFile.processLine(map);
            String result = moveFile.processLine(map);
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}