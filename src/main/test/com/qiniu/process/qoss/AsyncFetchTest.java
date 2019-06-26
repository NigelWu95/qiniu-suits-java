package com.qiniu.process.qoss;

import com.qiniu.config.PropertiesFile;
import com.qiniu.http.Response;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class AsyncFetchTest {

    @Test
    public void testSingleResult() throws IOException {

        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        String bucket = propertiesFile.getValue("bucket");
        BucketManager manager = new BucketManager(Auth.create(accessKey, secretKey), new Configuration());
        Response response = manager.asynFetch("http://p3l1d5mx4.bkt.clouddn.com/123456aaa.jpg ", bucket, "123456aaa.jpg -fetch");
        System.out.println(response.bodyString());
        response.close();
    }
}