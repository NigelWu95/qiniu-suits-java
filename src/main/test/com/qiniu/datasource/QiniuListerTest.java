package com.qiniu.datasource;

import com.qiniu.common.SuitsException;
import com.qiniu.config.ParamsConfig;
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
        IEntryParam entryParam = new ParamsConfig("resources" + System.getProperty("file.separator") + ".qiniu.properties");
        String accessKey = entryParam.getValue("ak");
        String secretKey = entryParam.getValue("sk");
        String bucket = entryParam.getValue("bucket");
        qiniuLister = new QiniuLister(new BucketManager(Auth.create(accessKey, secretKey), new Configuration()), bucket,
                null, null, null, null, 100);
    }

    @Test
    public void testHasFutureNext() {
        try {
            while (qiniuLister.hasFutureNext()) {
                System.out.println(true);
            }
        } catch (SuitsException e) {
            e.printStackTrace();
        }
    }
}