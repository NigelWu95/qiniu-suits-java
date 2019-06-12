package com.qiniu.datasource;

import com.qiniu.common.SuitsException;
import com.qiniu.config.ParamsConfig;
import com.qiniu.config.PropertiesFile;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class QiniuListerTest {

    private QiniuLister qiniuLister;

    @Before
    public void init() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        String bucket = propertiesFile.getValue("bucket");
        qiniuLister = new QiniuLister(new BucketManager(Auth.create(accessKey, secretKey), new Configuration()), bucket,
                "fragments/z1.yanyuvideo.room7", null, null, null, 10000);
    }

    @Test
    public void testNext() {
        int times = 0;
        int size = qiniuLister.currents().size();
        while (qiniuLister.hasNext()) {
            try {
                System.out.println(times++);
                qiniuLister.listForward();
                size += qiniuLister.currents().size();
            } catch (SuitsException e) {
                e.printStackTrace();
            }
        }
        System.out.println("over: " + size);
    }
}