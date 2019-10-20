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
import java.time.LocalDateTime;

public class QiniuListerTest {

    private QiniuLister qiniuLister;

    @Before
    public void init() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        String bucket = propertiesFile.getValue("bucket");
        qiniuLister = new QiniuLister(new BucketManager(Auth.create(accessKey, secretKey), new Configuration()), bucket,
                "fragments/z1.yanyuvideo.room7", null, null, 10000);
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

    @Test
    public void testFutureSize() {
        LocalDateTime time = LocalDateTime.now();
        System.out.println(futureSize(100));
        System.out.println(futureSize(500));
        System.out.println(futureSize(999));
        System.out.println(futureSize(1000));
        System.out.println(futureSize(2000));
        System.out.println(futureSize(3000));
        System.out.println(futureSize(3333));
        System.out.println(futureSize(4000));
        System.out.println(futureSize(4999));
        System.out.println(futureSize(5000));
        System.out.println(futureSize(5001));
        System.out.println(futureSize(10000));
        System.out.println(LocalDateTime.now().getNano() - time.getNano());
    }

    private int futureSize(int limit) {
        int expected = limit + 1;
        if (expected < 10000) expected = 10000 + 1;
        int times = 10;
        int futureSize = limit;
        if (limit < 1000) {
            futureSize += limit * 10;
        } else if (limit <= 5000) {
            futureSize += 10000;
        } else {
            futureSize += limit;
        }
        return futureSize;
    }
}