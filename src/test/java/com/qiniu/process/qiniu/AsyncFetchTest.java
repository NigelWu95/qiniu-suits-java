package com.qiniu.process.qiniu;

import com.qiniu.config.PropertiesFile;
import com.qiniu.storage.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

public class AsyncFetchTest {

    @Test
    public void testSingleResult() throws IOException {

        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        String bucket = propertiesFile.getValue("bucket");
        AsyncFetch asyncFetch = new AsyncFetch(accessKey, secretKey, new Configuration(), bucket, "http",
                "xxx.bkt.clouddn.com", null, null, null);
        asyncFetch.setFetchArgs("xxx.bkt.clouddn.com", null, "http://xxx.bkt.clouddn.com/",
                "key=$(key)&hash=$(etag)&w=$(imageInfo.width)&h=$(imageInfo.height)", null,
                "xxx.bkt.clouddn.com", 1, true);
        String result = asyncFetch.processLine(new HashMap<String, String>(){{
            put("url", "http://xxx.bkt.clouddn.com/123456aaa.jpg");
            put("key", "123456aaa.jpg");
        }});
        System.out.println(result);
    }
}