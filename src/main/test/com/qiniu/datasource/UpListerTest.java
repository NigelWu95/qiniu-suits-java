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
        String name = propertiesFile.getValue("up-name");
        String pass = propertiesFile.getValue("up-pass");
        UpYunClient upYunClient = new UpYunClient(new UpYunConfig(), name, pass);
        String bucket = propertiesFile.getValue("bucket");
        UpLister upLister = new UpLister(upYunClient, bucket, "wordSplit/xml/20161220/FF8080815919A15101591AFE37C603F7\t",
                null, null, 10000);
        String endKey = upLister.currentEndKey();
        System.out.println(endKey);
    }
}