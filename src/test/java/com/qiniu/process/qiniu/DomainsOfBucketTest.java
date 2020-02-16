package com.qiniu.process.qiniu;

import com.qiniu.config.PropertiesFile;
import com.qiniu.storage.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.*;

public class DomainsOfBucketTest {

    @Test
    public void testSingleResult() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        DomainsOfBucket domainsOfBucket = new DomainsOfBucket(accessKey, secretKey, new Configuration());
        String ret = domainsOfBucket.singleResult(new HashMap<String, String>(){{ put("bucket", "temp"); }});
        System.out.println(ret);
    }

}