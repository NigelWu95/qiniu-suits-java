package com.qiniu.datasource;

import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.obs.services.model.ObsObject;
import com.qiniu.config.PropertiesFile;
import com.qiniu.convert.JsonObjectPair;
import com.qiniu.util.CloudApiUtils;
import com.qiniu.util.ConvertingUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HuaweiListerTest {

    @Test
    public void testListing() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKeyId = propertiesFile.getValue("hua-id");
        String secretKey = propertiesFile.getValue("hua-secret");
        String bucket = propertiesFile.getValue("bucket");
        bucket = "css-backup-1544044401924";
        String endPoint = "https://obs." + CloudApiUtils.getHuaweiObsRegion(accessKeyId, secretKey, bucket) + ".myhuaweicloud.com";
        ObsConfiguration configuration = new ObsConfiguration();
        configuration.setEndPoint(endPoint);
        HuaweiLister huaweiLister = new HuaweiLister(new ObsClient(accessKeyId, secretKey, configuration), bucket,
                null, null, null, 10);
//        baiduLister.listForward();
        List<String> fields = new ArrayList<String>(ConvertingUtils.defaultFileFields){{
//            remove("mime");
            remove("status");
//            remove("md5");
        }};
        List<ObsObject> objectSummaries = huaweiLister.currents();
        for (ObsObject obsObject : objectSummaries) {
            System.out.println(ConvertingUtils.toPair(obsObject, fields, new JsonObjectPair()));
        }
    }
}