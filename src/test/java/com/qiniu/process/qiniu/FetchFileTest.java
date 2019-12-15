package com.qiniu.process.qiniu;

import com.qiniu.config.PropertiesFile;
import com.qiniu.storage.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class FetchFileTest {

    @Test
    public void testSingleProcess() throws IOException {

        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        FetchFile fetchFile = new FetchFile(accessKey, secretKey, new Configuration(), "nigel", "http",
                "pzg596hxa.bkt.clouddn.com", null, null, null);
//        fetchFile.setCheckType("stat");
        String result = fetchFile.processLine(new HashMap<String, String>(){{
            put("url", "http://pzg596hxa.bkt.clouddn.com/pom.xml");
            put("key", "pom.xml");
        }});
        System.out.println(result);
    }

    @Test
    public void testBatchProcess() throws IOException {

        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        FetchFile fetchFile = new FetchFile(accessKey, secretKey, new Configuration(), "nigel", "http",
                "pzg596hxa.bkt.clouddn.com", null, null, null, "/Users/wubingheng/Downloads/fetch");
        fetchFile.setCheckType("stat");
        List<Map<String, String>> list = new ArrayList<>();
        list.add(new HashMap<String, String>(){{
            put("url", "http://pzg596hxa.bkt.clouddn.com/pom.xml");
            put("key", "pom.xml");
        }});
        fetchFile.processLine(list);
        fetchFile.closeResource();
    }
}