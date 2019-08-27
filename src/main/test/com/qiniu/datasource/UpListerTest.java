package com.qiniu.datasource;

import com.qiniu.common.SuitsException;
import com.qiniu.config.PropertiesFile;
import com.qiniu.sdk.UpYunClient;
import com.qiniu.sdk.UpYunConfig;
import org.junit.Test;

import static org.junit.Assert.*;

public class UpListerTest {

    @Test
    public void testListForward() throws Exception {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String name = propertiesFile.getValue("up-id");
        String pass = propertiesFile.getValue("up-secret");
        String bucket = propertiesFile.getValue("bucket");
//        bucket = "squirrel";
        UpYunClient upYunClient = new UpYunClient(new UpYunConfig(), name, pass);
        UpLister upLister = new UpLister(upYunClient, bucket, "yflb/", null, null, 10000);
        String endKey = upLister.currentEndKey();
        System.out.println(endKey);
    }
}