package com.qiniu.process.qiniu;

import com.qiniu.config.PropertiesFile;
import com.qiniu.storage.Configuration;
import com.qiniu.util.StringMap;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class SyncUploadTest {

    @Test
    public void testSingleProcess() throws IOException {

        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        SyncUpload syncUpload = new SyncUpload(accessKey, secretKey, new Configuration(), "http",
                "pzg596hxa.bkt.clouddn.com", null, null, null, null, "nigel",
                360, new StringMap(), new StringMap());
        String result = syncUpload.processLine(new HashMap<String, String>(){{
            put("url", "http://pzg596hxa.bkt.clouddn.com/pom.xml");
            put("key", "pom1.xml");
        }});
        System.out.println(result);
    }

    @Test
    public void testBatchProcess() throws IOException {

        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        SyncUpload syncUpload = new SyncUpload(accessKey, secretKey, new Configuration(), "http",
                "pzg596hxa.bkt.clouddn.com", null, null, null, null, "nigel",
                360, new StringMap(), new StringMap(), "/Users/wubingheng/Downloads/syncupload");
        List<Map<String, String>> list = new ArrayList<>();
        list.add(new HashMap<String, String>(){{
            put("url", "http://pzg596hxa.bkt.clouddn.com/pom.xml");
            put("key", "pom2.xml");
        }});
        syncUpload.processLine(list);
        syncUpload.closeResource();
    }

}