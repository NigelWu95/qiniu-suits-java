package com.qiniu.datasource;

import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import com.baidubce.services.bos.model.BosObjectSummary;
import com.qiniu.config.PropertiesFile;
import com.qiniu.convert.JsonObjectPair;
import com.qiniu.util.ConvertingUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BaiduListerTest {

    @Test
    public void testListing() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKeyId = propertiesFile.getValue("bai-id");
        String secretKey = propertiesFile.getValue("bai-secret");
        String bucket = propertiesFile.getValue("bucket");
        bucket = "nigel-test";
        BosClient bosClient = new BosClient(new BosClientConfiguration().withEndpoint("su.bcebos.com")
                .withCredentials(new DefaultBceCredentials(accessKeyId, secretKey)));
        BaiduLister baiduLister = new BaiduLister(bosClient, bucket, null, null, null, 10000);
//        baiduLister.listForward();
        List<String> fields = new ArrayList<String>(ConvertingUtils.defaultFileFields){{
            remove("mime");
            remove("status");
            remove("md5");
        }};
        List<BosObjectSummary> objectSummaries = baiduLister.currents();
        for (BosObjectSummary objectSummary : objectSummaries) {
            System.out.println(ConvertingUtils.toPair(objectSummary, fields, new JsonObjectPair()));
        }
    }
}