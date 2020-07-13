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

public class RestoreArchiveTest {

    @Test
    public void testBatchResult() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        String bucket = propertiesFile.getValue("bucket");
        RestoreArchive restoreArchive = new RestoreArchive(accessKey, secretKey, new Configuration(), bucket, 1,
                "~/Downloads/restorear");
        List<Map<String, String>> list = new ArrayList<>();
        list.add(new HashMap<String, String>(){{ put("key", "qiniu_success_1.txt"); }});
        list.add(new HashMap<String, String>(){{ put("key", "qiniu_success_2.txt"); }});
        list.add(new HashMap<String, String>(){{ put("key", "qiniu_success_3.txt"); }});
        list.add(new HashMap<String, String>(){{ put("key", "qiniu_success_9.txt"); }});
        restoreArchive.setBatchSize(5);
        restoreArchive.processLine(list);
        restoreArchive.closeResource();
    }
}