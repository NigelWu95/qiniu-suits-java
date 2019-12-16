package com.qiniu.process.qiniu;

import com.qiniu.config.PropertiesFile;
import com.qiniu.storage.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ChangeMetadataTest {

    @Test
    public void testSingle() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        String bucket = propertiesFile.getValue("bucket");
        Map<String, String> metadata = new HashMap<>();
        metadata.put("test1", "test1");
        metadata.put("Cache-Control", "no store");
        ChangeMetadata changeMetadata = new ChangeMetadata(accessKey, secretKey, new Configuration(), bucket, metadata, null);
        Map<String, String> map = new HashMap<String, String>(){{
            put("key", "qiniu_success_1.txt");
        }};
        try {
            String result = changeMetadata.processLine(map);
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testBatch() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        String bucket = propertiesFile.getValue("bucket");
        Map<String, String> metadata = new HashMap<>();
        metadata.put("test1", "test1");
        metadata.put("Cache-Control", "no store");
        metadata.put("Last", "2019");
        ChangeMetadata changeMetadata = new ChangeMetadata(accessKey, secretKey, new Configuration(), bucket, metadata,
                null
                , "../temp"
        );
        List<Map<String, String>> list = new ArrayList<>();
        list.add(new HashMap<String, String>(){{ put("key", "qiniu_success_1.txt"); }});
        list.add(new HashMap<String, String>(){{ put("key", "qiniu_success_2.txt"); }});
        list.add(new HashMap<String, String>(){{ put("key", "qiniu_success_3.txt"); }});
        list.add(new HashMap<String, String>(){{ put("key", "qiniu_success_9.txt"); }});
        try {
            changeMetadata.processLine(list);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            changeMetadata.closeResource();
        }
    }
}