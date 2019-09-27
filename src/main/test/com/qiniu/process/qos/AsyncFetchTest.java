package com.qiniu.process.qos;

import com.qiniu.config.PropertiesFile;
import com.qiniu.http.Response;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
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
                "p3l1d5mx4.bkt.clouddn.com", null, null, null);
        asyncFetch.setFetchArgs("p3l1d5mx4.bkt.clouddn.com", null, "http://p3l1d5mx4.bkt.clouddn.com/",
                "key=$(key)&hash=$(etag)&w=$(imageInfo.width)&h=$(imageInfo.height)", null,
                "p3l1d5mx4.bkt.clouddn.com", 1, true);
        String result = asyncFetch.processLine(new HashMap<String, String>(){{
            put("url", "http://p3l1d5mx4.bkt.clouddn.com/123456aaa.jpg");
            put("key", "123456aaa.jpg");
        }});
        System.out.println(result);
    }
}